const http = require("http");
const https = require("https");
const crypto = require("crypto");
const { setupTracer, triggerTraceFromTraceparent } = require("./otel-hook-v4.js");

setupTracer();

const traceCallCounters = new Map();
let activeExecutionId = null;
let activeTraceId = null;
let lastRegisterTime = 0;
const LOCK_TTL_MS = 2 * 60 * 1000; // Verrou de 2 minutes par exécution

const MCP_SERVER_WORKFLOW_ID = "BMxUfKVV5C0rvQzo";

function deriveTraceId(seed) {
  return crypto.createHash("sha256").update(String(seed)).digest("hex").slice(0, 32);
}

function deriveSpanId(seed) {
  return crypto.createHash("sha256").update(String(seed) + "-span").digest("hex").slice(0, 16);
}

// 🔹 Détection et Prédiction Intelligente de l'Exécution Parent Active
async function getParentExecutionTraceId() {
  const now = Date.now();
  
  // 1. Si une exécution a été verrouillée il y a moins de 2 minutes, conserver son trace_id
  if (activeTraceId && (now - lastRegisterTime < LOCK_TTL_MS)) {
    return { traceId: activeTraceId, execId: activeExecutionId };
  }

  if (!process.env.N8N_HOST || !process.env.N8N_API_KEY) return null;

  try {
    const url = `${process.env.N8N_HOST.replace(/\/$/, "")}/api/v1/executions?limit=10`;
    const resp = await fetch(url, {
      headers: { "X-N8N-API-KEY": process.env.N8N_API_KEY, "Accept": "application/json" }
    });

    if (resp.ok) {
      const body = await resp.json();
      const executions = body.data || [];
      if (executions.length === 0) return null;

      // A. Vérifier si n8n signale déjà une exécution "running"
      const runningParent = executions.find(e => e.workflowId !== MCP_SERVER_WORKFLOW_ID && (e.status === 'running' || !e.stoppedAt));
      if (runningParent) {
        activeExecutionId = String(runningParent.id);
        activeTraceId = deriveTraceId(activeExecutionId);
        lastRegisterTime = now;
        console.log(`[proxy] 🟢 EXÉCUTION EN COURS DÉTECTÉE -> #${activeExecutionId}`);
        return { traceId: activeTraceId, execId: activeExecutionId };
      }

      // B. Trouver l'ID le plus élevé absolu dans la base de données (Parent ou Sub-workflow)
      const sortedAll = [...executions].sort((a, b) => Number(b.id) - Number(a.id));
      const maxExec = sortedAll[0];
      const maxId = Number(maxExec.id);
      const stoppedTime = maxExec.stoppedAt ? Date.parse(maxExec.stoppedAt) : now;

      // C. Prédiction Mathématique : Si la dernière exécution est terminée depuis > 3 secondes,
      // la requête entrante appartient à la NOUVELLE exécution qui démarre !
      if (now - stoppedTime > 3000) {
        let predictedParentId;
        if (maxExec.workflowId === MCP_SERVER_WORKFLOW_ID) {
          predictedParentId = maxId + 1; // Ex: #12536 (Sub) -> #12537 (Parent)
        } else {
          predictedParentId = maxId + 2; // Ex: #12535 (Parent) -> #12537 (Parent)
        }

        activeExecutionId = String(predictedParentId);
        activeTraceId = deriveTraceId(activeExecutionId);
        lastRegisterTime = now;
        console.log(`[proxy] ⚡ PRÉDICTION NOUVELLE EXÉCUTION -> Parent #${activeExecutionId} (Dernière connue en BDD: #${maxId})`);
        return { traceId: activeTraceId, execId: activeExecutionId };
      }

      // D. Fallback si terminée à l'instant
      const parentExecs = executions.filter(e => e.workflowId !== MCP_SERVER_WORKFLOW_ID).sort((a, b) => Number(b.id) - Number(a.id));
      if (parentExecs.length > 0) {
        activeExecutionId = String(parentExecs[0].id);
        activeTraceId = deriveTraceId(activeExecutionId);
        lastRegisterTime = now;
        console.log(`[proxy] 🟢 DERNIER PARENT -> #${activeExecutionId}`);
        return { traceId: activeTraceId, execId: activeExecutionId };
      }
    }
  } catch (e) {
    console.error("[proxy] Erreur API n8n:", e.message);
  }

  return null;
}

let lastUsage = {};

const server = http.createServer(async (req, res) => {
  const reqUrl = req.url || "/";
  console.log(`→ ${req.method} ${reqUrl}`);

  if (req.method === 'HEAD' || reqUrl === '/' || reqUrl === '/health') {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify({ status: 'ok' }));
    return;
  }

  if (reqUrl === '/last-usage') {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify(lastUsage));
    return;
  }

  if (reqUrl.includes('/models')) {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify({
      object: "list",
      data: [
        { id: "gemini-3.5-flash", object: "model", created: 1700000000, owned_by: "google" },
        { id: "gemini-embedding-001", object: "model", created: 1700000000, owned_by: "google" }
      ]
    }));
    return;
  }

  // 2. RÉSOLUTION DYNAMIQUE DE L'EXÉCUTION ACTIVE
  const syncInfo = await getParentExecutionTraceId();
  let traceId = syncInfo ? syncInfo.traceId : crypto.randomBytes(16).toString("hex");
  let execId = syncInfo ? syncInfo.execId : "unknown";

  if (!traceCallCounters.has(traceId)) {
    traceCallCounters.set(traceId, { chat: 0, embedding: 0, mcp: 0 });
    setTimeout(() => traceCallCounters.delete(traceId), 10 * 60 * 1000);
  }
  const counters = traceCallCounters.get(traceId);

  const isMcp = reqUrl.includes('/mcp-proxy');
  const isEmbedding = reqUrl.includes('/embeddings');

  let parentSpanId;
  let targetPath;

  if (isMcp) {
    targetPath = reqUrl.split('?')[0];
    parentSpanId = deriveSpanId(`${execId}_node_MCP Client1`);
    counters.mcp++;
  } else if (isEmbedding) {
    targetPath = '/ai-api/v1/embeddings';
    parentSpanId = deriveSpanId(`${execId}_node_Pinecone Vector Store`);
    counters.embedding++;
  } else {
    targetPath = '/ai-api/v1/chat/gemini';
    parentSpanId = deriveSpanId(`${execId}_node_AI Agent`);
    counters.chat++;
  }

  const unifiedTraceparent = `00-${traceId}-${parentSpanId}-01`;
  const headers = { ...req.headers };
  delete headers['authorization'];
  delete headers['Authorization'];
  delete headers['accept-encoding'];

  headers['traceparent'] = unifiedTraceparent;
  console.log(`→ Header transmis à Kong: ${unifiedTraceparent}`);

  triggerTraceFromTraceparent(unifiedTraceparent);

  const requestStartTime = Date.now();
  let reqChunks = [];

  req.on('data', chunk => reqChunks.push(chunk));
  req.on('end', () => {
    let reqBody = Buffer.concat(reqChunks);

    if (!isMcp) {
      try {
        const reqJson = JSON.parse(reqBody.toString());
        if (isEmbedding) {
          delete reqJson.encoding_format;
          reqJson.encoding_format = 'float';
        }

        const hasToolResult = reqJson.messages && reqJson.messages.some(m => m.role === 'tool');

        if (hasToolResult) {
          const systemMsg = reqJson.messages.find(m => m.role === 'system');
          const userMsg = reqJson.messages.find(m => m.role === 'user');
          const toolResults = reqJson.messages
            .filter(m => m.role === 'tool')
            .map(m => {
              try { return JSON.stringify(JSON.parse(m.content), null, 2); }
              catch(e) { return m.content; }
            }).join('\n\n');

          const newMessages = [];
          if (systemMsg) newMessages.push({ role: 'system', content: systemMsg.content });
          newMessages.push({
            role: 'user',
            content: `${userMsg?.content || ''}\n\n===RÉSULTATS PINECONE===\n${toolResults}\n========================`
          });
          reqJson.messages = newMessages;
          delete reqJson.tools;
        }

        const correctedBody = JSON.stringify(reqJson);
        reqBody = Buffer.from(correctedBody);
      } catch(e) {}
    }

    const options = {
      hostname: "35.198.99.79",
      port: 8443,
      path: targetPath,
      method: req.method,
      headers: { ...headers, 'content-length': reqBody.length },
      rejectUnauthorized: false
    };

    const proxy = https.request(options, (proxyRes) => {
      const realLatencyMs = Date.now() - requestStartTime;
      let chunks = [];

      proxyRes.on('data', chunk => chunks.push(chunk));
      proxyRes.on('end', () => {
        const body = Buffer.concat(chunks);

        if (!isMcp) {
          try {
            const json = JSON.parse(body.toString());
            if (json.usage && !isEmbedding && json.choices?.[0]?.message?.content) {
              lastUsage = {
                prompt_tokens: json.usage.prompt_tokens,
                completion_tokens: json.usage.completion_tokens,
                total_tokens: json.usage.total_tokens,
                latency_ms: realLatencyMs,
                model: json.model
              };
            }

            // Reconversion Float32LE -> Base64 pour n8n Embeddings OpenAI
            if (json.data && isEmbedding && Array.isArray(json.data[0]?.embedding)) {
              const emb = json.data[0].embedding;
              const buffer = Buffer.allocUnsafe(emb.length * 4);
              emb.forEach((val, i) => buffer.writeFloatLE(val, i * 4));
              json.data[0].embedding = buffer.toString('base64');

              const modBody = Buffer.from(JSON.stringify(json));
              const modHeaders = { ...proxyRes.headers, 'content-length': modBody.length };
              res.writeHead(proxyRes.statusCode, modHeaders);
              res.end(modBody);
              return;
            }
          } catch(e) {}
        }

        res.writeHead(proxyRes.statusCode, proxyRes.headers);
        res.end(body);
      });
    });

    proxy.on("error", (e) => {
      res.writeHead(500);
      res.end(e.message);
    });

    proxy.write(reqBody);
    proxy.end();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Proxy running on port ${PORT}`));
