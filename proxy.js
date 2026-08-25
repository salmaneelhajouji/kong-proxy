// proxy.js
const http = require("http");
const https = require("https");
const crypto = require("crypto");
const { setupTracer, triggerTraceFromTraceparent } = require("./otel-hook-v4.js");

setupTracer();

const traceCallCounters = new Map();
let activeExecutionId = null;
let activeTraceId = null;
let lastRegisterTime = 0;
const LOCK_TTL_MS = 2 * 60 * 1000;

const MCP_SERVER_WORKFLOW_ID = "BMxUfKVV5C0rvQzo";

// --- GESTION DU JETON AUTH0 (M2M) ---
let cachedAuth0Token = null;
let tokenExpiresAt = 0;

async function getAuth0Token() {
  const now = Date.now();
  if (cachedAuth0Token && now < tokenExpiresAt - 5 * 60 * 1000) {
    return cachedAuth0Token;
  }

  const domain = (process.env.AUTH0_DOMAIN || "").replace(/^https?:\/\//, "").replace(/\/$/, "");
  if (!domain || !process.env.AUTH0_CLIENT_SECRET) {
    return null;
  }

  try {
    const resp = await fetch(`https://${domain}/oauth/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        client_id: process.env.AUTH0_CLIENT_ID,
        client_secret: process.env.AUTH0_CLIENT_SECRET,
        audience: process.env.AUTH0_AUDIENCE,
        grant_type: 'client_credentials'
      })
    });

    if (resp.ok) {
      const data = await resp.json();
      cachedAuth0Token = data.access_token;
      tokenExpiresAt = now + (data.expires_in * 1000);
      console.log("🔒 [proxy] Jeton M2M Auth0 récupéré avec succès.");
      return cachedAuth0Token;
    } else {
      console.error("❌ [proxy] Erreur Auth0 :", await resp.text());
    }
  } catch (e) {
    console.error("❌ [proxy] Erreur d'appel Auth0 :", e.message);
  }
  return null;
}

function deriveTraceId(seed) {
  return crypto.createHash("sha256").update(String(seed)).digest("hex").slice(0, 32);
}

function deriveSpanId(seed) {
  return crypto.createHash("sha256").update(String(seed) + "-span").digest("hex").slice(0, 16);
}

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
      if (executions.length === 0) return null;

      const runningParent = executions.find(e => e.workflowId !== MCP_SERVER_WORKFLOW_ID && (e.status === 'running' || !e.stoppedAt));
      if (runningParent) {
        activeExecutionId = String(runningParent.id);
        activeTraceId = deriveTraceId(activeExecutionId);
        lastRegisterTime = now;
        return { traceId: activeTraceId, execId: activeExecutionId };
      }

      const sortedAll = [...executions].sort((a, b) => Number(b.id) - Number(a.id));
      const maxExec = sortedAll[0];
      const maxId = Number(maxExec.id);
      const stoppedTime = maxExec.stoppedAt ? Date.parse(maxExec.stoppedAt) : now;

      if (now - stoppedTime > 3000) {
        let predictedParentId = maxExec.workflowId === MCP_SERVER_WORKFLOW_ID ? maxId + 1 : maxId + 2;
        activeExecutionId = String(predictedParentId);
        activeTraceId = deriveTraceId(activeExecutionId);
        lastRegisterTime = now;
        return { traceId: activeTraceId, execId: activeExecutionId };
      }

      const parentExecs = executions.filter(e => e.workflowId !== MCP_SERVER_WORKFLOW_ID).sort((a, b) => Number(b.id) - Number(a.id));
      if (parentExecs.length > 0) {
        activeExecutionId = String(parentExecs[0].id);
        activeTraceId = deriveTraceId(activeExecutionId);
        lastRegisterTime = now;
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

  const isMcp = reqUrl.includes('/mcp-proxy');
  const isEmbedding = reqUrl.includes('/embeddings');

  let reqChunks = [];
  req.on('data', chunk => reqChunks.push(chunk));
  req.on('end', async () => {
    let reqBody = Buffer.concat(reqChunks);

    let jsonRpcMethod = null;
    if (isMcp && reqBody.length > 0) {
      try {
        const bodyObj = JSON.parse(reqBody.toString());
        jsonRpcMethod = bodyObj.method || null;
      } catch (e) {}
    }

    const isMcpToolCall = isMcp && req.method === 'POST' && jsonRpcMethod === "tools/call";
    const shouldInjectTrace = !isMcp || isMcpToolCall;

    const headers = { ...req.headers };
    delete headers['accept-encoding'];
    delete headers['traceparent'];

    // 🎯 INJECTION DU JETON AUTH0 UNIQUEMENT POUR LA ROUTE MCP
    if (isMcp) {
      const token = await getAuth0Token();
      if (token) {
        headers['authorization'] = `Bearer ${token}`;
      }
    }

    if (shouldInjectTrace) {
      const syncInfo = await getParentExecutionTraceId();
      let traceId = syncInfo ? syncInfo.traceId : crypto.randomBytes(16).toString("hex");
      let execId = syncInfo ? syncInfo.execId : "unknown";

      if (!traceCallCounters.has(traceId)) {
        traceCallCounters.set(traceId, { chat: 0, embedding: 0, mcp: 0 });
        setTimeout(() => traceCallCounters.delete(traceId), 10 * 60 * 1000);
      }
      const counters = traceCallCounters.get(traceId);

      let parentSpanId;
      if (isMcp) {
        parentSpanId = deriveSpanId(`${execId}_node_MCP Client1_${counters.mcp}`);
        counters.mcp++;
      } else if (isEmbedding) {
        parentSpanId = deriveSpanId(`${execId}_node_Embeddings OpenAI_${counters.embedding}`);
        counters.embedding++;
      } else {
        parentSpanId = deriveSpanId(`${execId}_node_LLM_${counters.chat}`);
        counters.chat++;
      }

      const unifiedTraceparent = `00-${traceId}-${parentSpanId}-01`;
      headers['traceparent'] = unifiedTraceparent;
      console.log(`→ [TRACÉ] ${req.method} ${reqUrl} | Method: ${jsonRpcMethod || 'N/A'} -> Header: ${unifiedTraceparent}`);

      triggerTraceFromTraceparent(unifiedTraceparent);
    } else {
      console.log(`🧹 [BRUIT FILTRÉ] ${req.method} ${reqUrl} | Method: ${jsonRpcMethod || 'GET/SSE'} -> Traceparent ignoré.`);
    }

    let targetPath;
    if (isMcp) {
      targetPath = reqUrl.split('?')[0];
    } else if (isEmbedding) {
      targetPath = '/ai-api/v1/embeddings';
    } else {
      targetPath = '/ai-api/v1/chat/gemini';
    }

    if (!isMcp && reqBody.length > 0) {
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

    const requestStartTime = Date.now();
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
