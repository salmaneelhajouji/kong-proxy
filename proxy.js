const https = require("https");
const http = require("http");
const crypto = require("crypto");
const { setupTracer, triggerTraceFromTraceparent } = require("./otel-hook-v4.js");
setupTracer();

// ✅ Compteurs d'appels HTTP par traceId pour dériver des Span IDs uniques
const traceCallCounters = new Map();

function deriveSpanId(seed) {
  return crypto.createHash("sha256").update(String(seed) + "-span").digest("hex").slice(0, 16);
}

let lastUsage = {};

const server = http.createServer((req, res) => {
  console.log(`→ ${req.method} ${req.url}`);

  const traceparentHeader = req.headers['traceparent'];
  if (traceparentHeader) {
    console.log(`[trace-hook] traceparent reçu: ${traceparentHeader}`);
    triggerTraceFromTraceparent(traceparentHeader);
  }

  // ✅ Health check
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

  const headers = {...req.headers};
  delete headers['authorization'];
  delete headers['Authorization'];
  delete headers['accept-encoding'];

  let targetPath;
  if (req.url.includes('/embeddings')) {
    targetPath = '/ai-api/v1/embeddings';
    console.log(`→ Kong Embeddings: ${targetPath}`);
  } else {
    targetPath = '/ai-api/v1/chat/gemini';
    console.log(`→ Kong Chat: ${targetPath}`);
  }

  const isEmbedding = targetPath.includes('/embeddings');
  const requestStartTime = Date.now();

  // 🔹 Réécriture dynamique du traceparent transmis à Kong
  if (traceparentHeader) {
    const parts = traceparentHeader.split("-");
    if (parts.length >= 4) {
      const traceId = parts[1];

      if (!traceCallCounters.has(traceId)) {
        traceCallCounters.set(traceId, { chat: 0, embedding: 0 });
        setTimeout(() => traceCallCounters.delete(traceId), 10 * 60 * 1000);
      }
      const counters = traceCallCounters.get(traceId);

      let derivedSpanId;
      if (isEmbedding) {
        derivedSpanId = deriveSpanId(traceId + `-embeddings-${counters.embedding}`);
        counters.embedding++;
      } else {
        derivedSpanId = deriveSpanId(traceId + `-chat-${counters.chat}`);
        counters.chat++;
      }

      headers['traceparent'] = `00-${traceId}-${derivedSpanId}-01`;
      console.log(`→ Header traceparent réécrit pour Kong: 00-${traceId}-${derivedSpanId}-01`);
    }
  }

  let reqChunks = [];
  req.on('data', chunk => reqChunks.push(chunk));
  req.on('end', () => {
    let reqBody = Buffer.concat(reqChunks);

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
