const https = require("https");
const http = require("http");

const server = http.createServer((req, res) => {
  console.log(`→ ${req.method} ${req.url}`);

  // ✅ Bypass serveo warning
  if (req.url.includes('serveo-skip-browser-warning')) {
    const url = new URL(req.url, 'http://localhost');
    req.url = url.pathname;
  }
  // ✅ Health check pour Render
  if (req.method === 'HEAD' || req.url === '/health' || req.url === '/') {
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

  // ✅ Routing selon le chemin
  let targetPath;
  if (req.url.includes('/embeddings')) {
    targetPath = '/ai-api/v1/embeddings';
    console.log(`→ Kong Embeddings: ${targetPath}`);
  } else {
    targetPath = '/ai-api/v1/chat/gemini';
    console.log(`→ Kong Chat: ${targetPath}`);
  }

  // ✅ Collecte le body de la requête
  let reqChunks = [];
  req.on('data', chunk => reqChunks.push(chunk));
  req.on('end', () => {
    let reqBody = Buffer.concat(reqChunks);

    try {
      const reqJson = JSON.parse(reqBody.toString());

      // ✅ Fix 1 — Force encoding_format float pour embeddings
      if (targetPath.includes('/embeddings')) {
        if (reqJson.encoding_format === 'base64') {
          console.log(`→ encoding_format base64 → float`);
        }
        reqJson.encoding_format = 'float';
        console.log(`→ encoding_format forcé à float`);
      }

      // ✅ Fix 2 — Détecte le 2ème appel avec tool results
      const hasToolResult = reqJson.messages && 
                            reqJson.messages.some(m => m.role === 'tool');

      if (hasToolResult) {
        console.log(`→ Détection 2ème appel avec tool results`);

        const systemMsg = reqJson.messages.find(m => m.role === 'system');
        const userMsg = reqJson.messages.find(m => m.role === 'user');

        const toolResults = reqJson.messages
          .filter(m => m.role === 'tool')
          .map(m => {
            try {
              const parsed = JSON.parse(m.content);
              return JSON.stringify(parsed, null, 2);
            } catch(e) {
              return m.content;
            }
          })
          .join('\n\n');

        console.log(`→ Tool results récupérés:\n${toolResults.slice(0, 300)}`);

        const newMessages = [];
        if (systemMsg) {
          newMessages.push({ role: 'system', content: systemMsg.content });
        }
        newMessages.push({
          role: 'user',
          content: `${userMsg?.content || ''}\n\n===RÉSULTATS PINECONE===\n${toolResults}\n========================`
        });

        reqJson.messages = newMessages;
        delete reqJson.tools;

        console.log(`→ Messages reconstruits sans tool calls`);
        console.log(`→ Nouveau body user:\n${newMessages[newMessages.length-1].content.slice(0, 400)}`);

      } else if (reqJson.messages) {
        reqJson.messages = reqJson.messages.map(msg => {
          if (msg.role === 'assistant' && msg.tool_calls) {
            msg.tool_calls = msg.tool_calls.map(tc => {
              if (tc.function && tc.function.arguments && Array.isArray(tc.function.arguments)) {
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
      headers: {
        ...headers,
        'content-length': reqBody.length
      },
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
            console.log(`← Type embedding: ${typeof emb}`);
            console.log(`← Est array: ${Array.isArray(emb)}`);
            console.log(`← Premiers éléments: ${JSON.stringify(emb)?.slice(0, 100)}`);

            if (typeof emb === 'string') {
              console.log(`← Embedding en base64 — décodage en float32`);
              const buffer = Buffer.from(emb, 'base64');
              const floats = [];
              for (let i = 0; i < buffer.length; i += 4) {
                floats.push(buffer.readFloatLE(i));
              }
              console.log(`← Embedding dimensions après décodage: ${floats.length}`);
              json.data[0].embedding = floats;

              const correctedResponse = JSON.stringify(json);
              res.writeHead(proxyRes.statusCode, proxyRes.headers);
              res.end(correctedResponse);
              return;
            }

            console.log(`← Embedding dimensions réelles : ${Array.isArray(emb) ? emb.length : 'N/A'}`);
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

server.listen(3000, () => console.log("Proxy running on http://localhost:3000"));
