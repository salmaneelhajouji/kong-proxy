const http = require("http");
const https = require("https");
const crypto = require("crypto");
const { setupTracer, triggerTraceFromTraceparent } = require("./otel-hook-v4.js");

setupTracer();

const traceCallCounters = new Map();
let activeExecutionId = null;
let activeTraceId = null;
let lastRegisterTime = 0;
const LOCK_TTL_MS = 5 * 60 * 1000;

const MCP_SERVER_WORKFLOW_ID = "BMxUfKVV5C0rvQzo";

function deriveTraceId(seed) {
  return crypto.createHash("sha256").update(String(seed)).digest("hex").slice(0, 32);
}

function deriveSpanId(seed) {
  return crypto.createHash("sha256").update(String(seed) + "-span").digest("hex").slice(0, 16);
}

// 🔹 Fallback via API n8n si aucun ID valide n'est fourni
async function getParentExecutionTraceId() {
  const now = Date.now();
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
      const parentExecutions = executions.filter(e => e.workflowId !== MCP_SERVER_WORKFLOW_ID);
      const activeRunning = parentExecutions.find(e => e.status === 'running');
      const latestExec = activeRunning || parentExecutions.sort((a, b) => Number(b.id) - Number(a.id))[0];

      if (latestExec) {
        activeExecutionId = String(latestExec.id);
        activeTraceId = deriveTraceId(activeExecutionId);
        lastRegisterTime = now;
        return { traceId: activeTraceId, execId: activeExecutionId };
      }
    }
  } catch (e) {
    console.error("[proxy] Erreur fetch executions n8n:", e.message);
  }

  return null;
}

let lastUsage = {};

const server = http.createServer(async (req, res) => {
  const reqUrl = req.url || "/";
  console.log(`→ ${req.method} ${reqUrl}`);

  // 1. EXTRACTION & NETTOYAGE STRICT DE L'EXEC_ID (Conservation uniquement des chiffres)
  const parsedUrl = new URL(reqUrl, `http://${req.headers.host || 'localhost'}`);
  let rawExecId = parsedUrl.searchParams.get('exec_id') || parsedUrl.searchParams.get('id');

  if (rawExecId) {
    const cleanIdMatch = rawExecId.match(/\d+/);
    if (cleanIdMatch) {
      activeExecutionId = cleanIdMatch[0];
      activeTraceId = deriveTraceId(activeExecutionId);
      lastRegisterTime = Date.now();
      console.log(`[proxy] 🎯 EXEC_ID NETTOYÉ EN DIRECT -> #${activeExecutionId} | trace_id: ${activeTraceId}`);
    }
  }

  if (reqUrl.includes('/register-execution') && activeExecutionId) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'registered', executionId: activeExecutionId, traceId: activeTraceId }));
    return;
  }

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

  // 2. RÉSOLVEUR DE TRACE UNIFIÉE
  let traceId = activeTraceId;
  let execId = activeExecutionId;

  if (!traceId) {
    const syncInfo = await getParentExecutionTraceId();
    traceId = syncInfo ? syncInfo.traceId : crypto.randomBytes(16).toString("hex");
    execId = syncInfo ? syncInfo.execId : "unknown";
  }

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

            // 🔹 RECONVERSION EN BASE64 POUR N8N (Résout l'erreur des zéro `[0,0,0...]`)
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
