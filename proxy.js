const https = require("https");
const http = require("http");

// ✅ Stockage en mémoire des derniers tokens et latence réels
let lastUsage = {};

const server = http.createServer((req, res) => {
  console.log(`→ ${req.method} ${req.url}`);

  // ✅ Health check pour Render
  if (req.method === 'HEAD' || req.url === '/' || req.url === '/health') {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify({ status: 'ok' }));
    return;
  }

  // ✅ Route pour récupérer les derniers tokens + latence réels
  if (req.url === '/last-usage') {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify(lastUsage));
    return;
  }

  // ✅ Fake /v1/models
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
  const requestStartTime = Date.now(); // ✅ Mesure le début réel de l'appel

  let reqChunks = [];
  req.on('data', chunk => reqChunks.push(chunk));
  req.on('end', () => {
    let reqBody = Buffer.concat(reqChunks);

    try {
      const reqJson = JSON.parse(reqBody.toString());

      // ✅ Fix embeddings — force float
      if (isEmbedding) {
        delete reqJson.encoding_format;
        reqJson.encoding_format = 'float';
        console.log(`→ encoding_format forcé à float`);
      }

      // ✅ Fix tool results
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
      const requestEndTime = Date.now(); // ✅ Mesure la fin réelle de l'appel
      const realLatencyMs = requestEndTime - requestStartTime;
      console.log(`← Kong status: ${proxyRes.statusCode} | Latence réelle: ${realLatencyMs}ms`);

      let chunks = [];
      proxyRes.on('data', chunk => chunks.push(chunk));
      proxyRes.on('end', () => {
        const body = Buffer.concat(chunks);

        try {
          const json = JSON.parse(body.toString());
          if (json.model) console.log(`← Modèle : ${json.model}`);
          if (json.usage) console.log(`← Tokens : prompt=${json.usage?.prompt_tokens} completion=${json.usage?.completion_tokens} total=${json.usage?.total_tokens}`);

          // ✅ Stocke les vrais tokens + latence du dernier appel CHAT (pas embedding)
          if (json.usage && !isEmbedding) {
            lastUsage = {
              prompt_tokens: json.usage.prompt_tokens,
              completion_tokens: json.usage.completion_tokens,
              total_tokens: json.usage.total_tokens,
              latency_ms: realLatencyMs,
              model: json.model,
              timestamp: Date.now()
            };
            console.log(`← Usage stocké: ${JSON.stringify(lastUsage)}`);
          }

          // ✅ Fix embedding — ré-encode 3072 floats en base64
          if (json.data && isEmbedding) {
            const emb = json.data[0]?.embedding;
            console.log(`← Embedding type: ${typeof emb} | array: ${Array.isArray(emb)} | dims: ${Array.isArray(emb) ? emb.length : 'N/A'}`);

            if (Array.isArray(emb)) {
              const buffer = Buffer.allocUnsafe(emb.length * 4);
              emb.forEach((val, i) => buffer.writeFloatLE(val, i * 4));
              json.data[0].embedding = buffer.toString('base64');
              console.log(`← Ré-encodé ${emb.length} floats → base64 (${buffer.length} bytes)`);
              res.writeHead(proxyRes.statusCode, proxyRes.headers);
              res.end(JSON.stringify(json));
              return;
            }
          }

          if (proxyRes.statusCode === 400) {
            console.log(`← Erreur 400: ${body.toString().slice(0, 500)}`);
          }

        } catch(e) {
          console.log(`← Body raw: ${body.toString().slice(0, 200)}`);
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
