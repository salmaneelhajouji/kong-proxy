const https = require("https");
const http = require("http");

const server = http.createServer((req, res) => {
  console.log(`→ ${req.method} ${req.url}`);

  // ✅ Health check pour Render
  if (req.method === 'HEAD' || req.url === '/' || req.url === '/health') {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify({ status: 'ok' }));
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

  let reqChunks = [];
  req.on('data', chunk => reqChunks.push(chunk));
  req.on('end', () => {
    let reqBody = Buffer.concat(reqChunks);

    try {
      const reqJson = JSON.parse(reqBody.toString());

      // ✅ Fix 1 — Force encoding_format float pour embeddings
      if (targetPath.includes('/embeddings')) {
        reqJson.encoding_format = 'float';
        console.log(`→ encoding_format forcé à float`);
      }

      // ✅ Fix 2 — Détecte le 2ème appel avec tool results
      const hasToolResult = reqJson.messages &&
                            reqJson.messages.some(m => m.role === 'tool');

      if (hasToolResult) {
        console.log(`→ Détection 2ème appel avec tool results`);

        const systemMsg = reqJson.messages.find(m => m.role === 'system');
        const userMsg   = reqJson.messages.find(m => m.role === 'user');

        const toolResults = reqJson.messages
          .filter(m => m.role === 'tool')
          .map(m => {
            try {
              return JSON.stringify(JSON.parse(m.content), null, 2);
            } catch(e) {
              return m.content;
            }
          })
          .join('\n\n');

        console.log(`→ Tool results:\n${toolResults.slice(0, 300)}`);

        const newMessages = [];
        if (systemMsg) newMessages.push({ role: 'system', content: systemMsg.content });
        newMessages.push({
          role: 'user',
          content: `${userMsg?.content || ''}\n\n===RÉSULTATS PINECONE===\n${toolResults}\n========================`
        });

        reqJson.messages = newMessages;
        delete reqJson.tools;
        console.log(`→ Messages reconstruits sans tool calls`);

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
      console.log(`→ Body envoyé à Kong:\n${correctedBody.slice(0, 400)}`);
      reqBody = Buffer.from(correctedBody);

    } catch(e) {
      console.log(`→ Body non-JSON, envoi tel quel`);
    }

    const isEmbedding = targetPath.includes('/embeddings');

    const options = {
      hostname: "35.198.99.79",
      port: 8443,
      path: targetPath,
      method: req.method,
      headers: { ...headers, 'content-length': reqBody.length },
      rejectUnauthorized: false
    };

    const proxy = https.request(options, (proxyRes) => {
      console.log(`← Kong status: ${proxyRes.statusCode}`);

      let chunks = [];
      proxyRes.on('data', chunk => chunks.push(chunk));
      proxyRes.on('end', () => {
        const body = Buffer.concat(chunks);

        try {
          const json = JSON.parse(body.toString());
          if (json.model) console.log(`← Modèle : ${json.model}`);
          if (json.usage) console.log(`← Tokens : prompt=${json.usage?.prompt_tokens} completion=${json.usage?.completion_tokens} total=${json.usage?.total_tokens}`);

          if (json.data && isEmbedding) {
            const emb = json.data[0]?.embedding;
            console.log(`← Type embedding: ${typeof emb} | Array: ${Array.isArray(emb)}`);

            if (typeof emb === 'string') {
              console.log(`← Décodage base64 → float32`);
              const buffer = Buffer.from(emb, 'base64');
              const floats = [];
              for (let i = 0; i < buffer.length; i += 4) {
                floats.push(buffer.readFloatLE(i));
              }
              console.log(`← Dimensions après décodage: ${floats.length}`);
              json.data[0].embedding = floats;
              res.writeHead(proxyRes.statusCode, proxyRes.headers);
              res.end(JSON.stringify(json));
              return;
            }

            console.log(`← Dimensions: ${Array.isArray(emb) ? emb.length : 'N/A'}`);
          }

          if (proxyRes.statusCode === 400) {
            console.log(`← Erreur 400: ${body.toString().slice(0, 500)}`);
          }

        } catch(e) {
          console.log(`← Body (raw): ${body.toString().slice(0, 200)}`);
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
