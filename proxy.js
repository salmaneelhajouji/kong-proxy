const https = require("https");
const http = require("http");
const crypto = require("crypto");
const { setupTracer, triggerTraceFromTraceparent } = require("./otel-hook-v4.js");
setupTracer();

const traceCallCounters = new Map();

let lockedMainTraceId = null;
let lastLockTime = 0;
const LOCK_TTL_MS = 3 * 60 * 1000; // Valide 3 minutes par exécution de workflow

function deriveTraceId(seed) {
  return crypto.createHash("sha256").update(String(seed)).digest("hex").slice(0, 32);
}

function deriveSpanId(seed) {
  return crypto.createHash("sha256").update(String(seed) + "-span").digest("hex").slice(0, 16);
}

// 🔹 Résolution stricte : On ignore tout sous-workflow MCP Server
async function resolveMainWorkflowTraceId() {
  if (!process.env.N8N_HOST || !process.env.N8N_API_KEY) return null;

  try {
    const url = `${process.env.N8N_HOST.replace(/\/$/, "")}/api/v1/executions?limit=15`;
    const resp = await fetch(url, {
      headers: { "X-N8N-API-KEY": process.env.N8N_API_KEY, "Accept": "application/json" }
    });
    if (resp.ok) {
      const body = await resp.json();
      const executions = body.data || [];
      
      // 🎯 RÈGLE STRICTE : Exclure "MCP Server" et prendre l'exécution Parent (mode webhook/manual)
      const parentExec = executions.find(e => {
        const name = (e.workflowData?.name || "").toLowerCase();
        const isSub = name.includes("mcp server") || e.mode === "subworkflow" || e.mode === "integrated";
        return !isSub && (name.includes("agent") || e.mode === "webhook" || e.mode === "manual");
      }) || executions.find(e => !(e.workflowData?.name || "").toLowerCase().includes("mcp server"));

      if (parentExec) {
        console.log(`[proxy] Workflow Parent RACINE identifié : #${parentExec.id} (${parentExec.workflowData?.name})`);
        return deriveTraceId(parentExec.id);
      }
    }
  } catch (e) {
    console.error("[proxy] Erreur de résolution du workflow racine:", e.message);
  }
  return null;
}

let lastUsage = {};

const server = http.createServer(async (req, res) => {
  console.log(`→ ${req.method} ${req.url}`);

  if (req.method === 'HEAD' || req.url === '/' || req.url === '/health') {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify({ status: 'ok' }));
    return;
  }

  if (req.url === '/last-usage') {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify(lastUsage));
    return;
  }

  if (req.url.includes('/models')) {
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

  const now = Date.now();
  let incomingTraceparent = req.headers['traceparent'];
  let traceId = null;

  // 1️⃣ Si un traceparent entrant existe (provenant de n8n)
  if (incomingTraceparent) {
    const parts = incomingTraceparent.split("-");
    if (parts.length >= 4) {
      traceId = parts[1];
      lockedMainTraceId = traceId;
      lastLockTime = now;
      console.log(`[proxy] Trace Racine verrouillée via header entrant : ${traceId}`);
    }
  }

  // 2️⃣ Si pas de traceparent (ex: /mcp-proxy), réutiliser la Trace Racine verrouillée
  if (!traceId && lockedMainTraceId && (now - lastLockTime < LOCK_TTL_MS)) {
    traceId = lockedMainTraceId;
    console.log(`[proxy] Injection du Trace ID Racine sur l'appel MCP : ${traceId}`);
  }

  // 3️⃣ En dernier recours, chercher l'exécution parent Agent Discovery dans l'API n8n
  if (!traceId) {
    traceId = await resolveMainWorkflowTraceId();
    if (traceId) {
      lockedMainTraceId = traceId;
      lastLockTime = now;
    } else {
      traceId = crypto.randomBytes(16).toString("hex");
    }
  }

  if (!traceCallCounters.has(traceId)) {
    traceCallCounters.set(traceId, { chat: 0, embedding: 0, mcp: 0 });
    setTimeout(() => traceCallCounters.delete(traceId), 10 * 60 * 1000);
  }
  const counters = traceCallCounters.get(traceId);

  const isMcp = req.url.includes('/mcp-proxy');
  const isEmbedding = req.url.includes('/embeddings');

  let derivedSpanId;
  let targetPath;

  if (isMcp) {
    targetPath = req.url;
    derivedSpanId = deriveSpanId(traceId + `-mcp-${counters.mcp}`);
    counters.mcp++;
    console.log(`→ Kong MCP Proxy: ${targetPath}`);
  } else if (isEmbedding) {
    targetPath = '/ai-api/v1/embeddings';
    derivedSpanId = deriveSpanId(traceId + `-embeddings-${counters.embedding}`);
    counters.embedding++;
    console.log(`→ Kong Embeddings: ${targetPath}`);
  } else {
    targetPath = '/ai-api/v1/chat/gemini';
    derivedSpanId = deriveSpanId(traceId + `-chat-${counters.chat}`);
    counters.chat++;
    console.log(`→ Kong Chat: ${targetPath}`);
  }

  const unifiedTraceparent = `00-${traceId}-${derivedSpanId}-01`;
  const headers = {...req.headers};
  delete headers['authorization'];
  delete headers['Authorization'];
  delete headers['accept-encoding'];

  headers['traceparent'] = unifiedTraceparent;
  console.log(`→ Header traceparent UNIFIÉ transmis à Kong: ${unifiedTraceparent}`);

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

        const hasToolResult = reqJson.messages &&
                              reqJson.messages.some(m => m.role === 'tool');

        if (hasToolResult) {
          console.log(`→ Détection 2ème appel avec tool results`);
          const systemMsg = reqJson.messages.find(m => m.role === 'system');
          const userMsg   = reqJson.messages.find(m => m.role === 'user');
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

        } else if (reqJson.messages) {
          reqJson.messages = reqJson.messages.map(msg => {
            if (msg.role === 'assistant' && msg.tool_calls) {
              msg.tool_calls = msg.tool_calls.map(tc => {
                if (tc.function?.arguments && Array.isArray(tc.function.arguments)) {
                  tc.function.arguments = JSON.stringify(tc.function.arguments);
                }
                return tc;
              });
            }
            return msg;
          });
        }

        const correctedBody = JSON.stringify(reqJson);
        reqBody = Buffer.from(correctedBody);

      } catch(e) {
        console.log(`→ Body non-JSON`);
      }
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
      const requestEndTime = Date.now();
      const realLatencyMs = requestEndTime - requestStartTime;
      console.log(`← Kong status: ${proxyRes.statusCode} | Latence réelle: ${realLatencyMs}ms`);

      let chunks = [];
      proxyRes.on('data', chunk => chunks.push(chunk));
      proxyRes.on('end', () => {
        const body = Buffer.concat(chunks);

        if (!isMcp) {
          try {
            const json = JSON.parse(body.toString());

            if (json.usage && !isEmbedding && json.choices?.[0]?.message?.content) {
              const usagePayload = {
                prompt_tokens: json.usage.prompt_tokens,
                completion_tokens: json.usage.completion_tokens,
                total_tokens: json.usage.total_tokens,
                thinking_tokens: json.usage.total_tokens - json.usage.prompt_tokens - json.usage.completion_tokens,
                latency_ms: realLatencyMs,
                model: json.model
              };
              lastUsage = usagePayload;
            }

            if (json.data && isEmbedding) {
              const emb = json.data[0]?.embedding;
              if (Array.isArray(emb)) {
                const buffer = Buffer.allocUnsafe(emb.length * 4);
                emb.forEach((val, i) => buffer.writeFloatLE(val, i * 4));
                json.data[0].embedding = buffer.toString('base64');
                res.writeHead(proxyRes.statusCode, proxyRes.headers);
                res.end(JSON.stringify(json));
                return;
              }
            }

          } catch(e) {}
        }

        res.writeHead(proxyRes.statusCode, proxyRes.headers);
        res.end(body);
      });
    });

    proxy.on("error", (e) => {
      console.log(`← Erreur: ${e.message}`);
      res.writeHead(500);
      res.end(e.message);
    });

    proxy.write(reqBody);
    proxy.end();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Proxy running on port ${PORT}`));
