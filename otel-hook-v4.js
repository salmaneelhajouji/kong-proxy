const crypto = require("crypto");
const { trace, context } = require("@opentelemetry/api");
const { BasicTracerProvider, BatchSpanProcessor } = require("@opentelemetry/sdk-trace-node");
const { OTLPTraceExporter } = require("@opentelemetry/exporter-trace-otlp-http");
const { Resource } = require("@opentelemetry/resources");
const { SemanticResourceAttributes } = require("@opentelemetry/semantic-conventions");

// ── Config ───────────────────────────────────────────────────────────────────
const N8N_HOST = process.env.N8N_HOST;
const N8N_API_KEY = process.env.N8N_API_KEY;
const OTEL_ENDPOINT = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
const OTEL_HEADERS_RAW = process.env.OTEL_EXPORTER_OTLP_HEADERS || "";
const REVERSE_SEARCH_RANGE = parseInt(process.env.REVERSE_SEARCH_RANGE || "300", 10);
const MAX_ATTR_LENGTH = parseInt(process.env.MAX_ATTR_LENGTH || "8000", 10);

function parseHeaders(raw) {
  const result = {};
  if (!raw.trim()) return result;
  raw.split(",").forEach(pair => {
    const idx = pair.indexOf("=");
    if (idx > -1) {
      result[pair.slice(0, idx).trim()] = pair.slice(idx + 1).trim();
    }
  });
  return result;
}

// ── Custom IdGenerator OpenTelemetry ─────────────────────────────────────────
let forcedTraceId = null;
let forcedNextSpanId = null;

const customIdGenerator = {
  generateTraceId: () => {
    if (forcedTraceId) {
      const tid = forcedTraceId;
      forcedTraceId = null;
      return tid;
    }
    return crypto.randomBytes(16).toString("hex");
  },
  generateSpanId: () => {
    if (forcedNextSpanId) {
      const sid = forcedNextSpanId;
      forcedNextSpanId = null;
      return sid;
    }
    return crypto.randomBytes(8).toString("hex");
  }
};

let tracer = null;

function setupTracer() {
  const resource = new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: "n8n-agentic4api-webhook",
  });

  const exporter = new OTLPTraceExporter({
    url: OTEL_ENDPOINT,
    headers: parseHeaders(OTEL_HEADERS_RAW),
  });

  const provider = new BasicTracerProvider({
    resource,
    idGenerator: customIdGenerator,
  });

  provider.addSpanProcessor(new BatchSpanProcessor(exporter));
  provider.register();

  tracer = trace.getTracer("n8n-webhook-hook");
  console.log(`[otel] Tracer initialisé -> ${OTEL_ENDPOINT}`);
}

function deriveTraceId(seed) {
  return crypto.createHash("sha256").update(String(seed)).digest("hex").slice(0, 32);
}

function deriveSpanId(seed) {
  return crypto.createHash("sha256").update(String(seed) + "-span").digest("hex").slice(0, 16);
}

// ── Reverse lookup: trace_id -> execution_id ─────────────────────────────────
let lastKnownExecutionId = null;

async function fetchLatestExecutionId() {
  const url = `${N8N_HOST.replace(/\/$/, "")}/api/v1/executions?limit=1`;
  const resp = await fetch(url, {
    headers: { "X-N8N-API-KEY": N8N_API_KEY, "Accept": "application/json" },
  });
  if (!resp.ok) throw new Error(`n8n API error ${resp.status}`);
  const body = await resp.json();
  const latest = body.data?.[0]?.id;
  return latest ? parseInt(latest, 10) : null;
}

async function resolveExecutionIdFromTraceId(traceId) {
  if (lastKnownExecutionId === null) {
    try {
      lastKnownExecutionId = await fetchLatestExecutionId();
    } catch (e) {
      console.error(`[otel] Impossible de récupérer le dernier execution_id:`, e.message);
      return null;
    }
  }

  const upperBound = lastKnownExecutionId + 20;
  const lowerBound = Math.max(1, lastKnownExecutionId - REVERSE_SEARCH_RANGE);

  for (let candidate = upperBound; candidate >= lowerBound; candidate--) {
    if (deriveTraceId(candidate) === traceId) {
      lastKnownExecutionId = Math.max(lastKnownExecutionId, candidate);
      return candidate;
    }
  }

  console.warn(`[otel] Aucun execution_id trouvé pour trace_id=${traceId} dans la plage [${lowerBound}, ${upperBound}]`);
  return null;
}

async function n8nGet(path) {
  const url = `${N8N_HOST.replace(/\/$/, "")}/api/v1${path}`;
  const resp = await fetch(url, {
    headers: {
      "X-N8N-API-KEY": N8N_API_KEY,
      "Accept": "application/json",
    },
  });
  if (!resp.ok) {
    throw new Error(`n8n API error ${resp.status}: ${await resp.text()}`);
  }
  return resp.json();
}

function toOtelStatusCode(n8nStatus) {
  if (n8nStatus === "success") return 1;
  if (n8nStatus === "error") return 2;
  return 0;
}

function truncate(value) {
  const str = typeof value === "string" ? value : JSON.stringify(value);
  if (!str) return str;
  return str.length > MAX_ATTR_LENGTH ? str.slice(0, MAX_ATTR_LENGTH) + "... [tronqué]" : str;
}

function extractFirstJson(container, key) {
  const branch = container?.[key];
  if (!Array.isArray(branch) || !Array.isArray(branch[0]) || !branch[0][0]) return null;
  return branch[0][0].json ?? null;
}

function extractIO(nodeRun) {
  let inputData = null;
  let outputData = null;

  if (nodeRun.inputOverride) {
    for (const key of Object.keys(nodeRun.inputOverride)) {
      const json = extractFirstJson(nodeRun.inputOverride, key);
      if (json) { inputData = json; break; }
    }
  }

  if (nodeRun.data) {
    if (nodeRun.data.main) {
      const json = extractFirstJson(nodeRun.data, "main");
      if (json) { outputData = json; }
    } else {
      for (const key of Object.keys(nodeRun.data)) {
        const json = extractFirstJson(nodeRun.data, key);
        if (json) { outputData = json; break; }
      }
    }
  }

  return { inputData, outputData };
}

function parseExecution(detail) {
  const nodeTypeLookup = {};
  (detail.workflowData?.nodes || []).forEach(node => {
    nodeTypeLookup[node.name] = node.type;
  });

  const resultData = detail.data?.resultData || {};
  const error = resultData.error;

  const parent = {
    executionId: String(detail.id),
    workflowId: detail.workflowId,
    workflowName: detail.workflowData?.name || "unknown-workflow",
    startedAt: detail.startedAt,
    stoppedAt: detail.stoppedAt,
    status: detail.status,
    mode: detail.mode,
    retryOf: detail.retryOf,
    errorMessage: error?.message || null,
    errorNode: error?.node?.name || null,
  };

  const runData = resultData.runData || {};
  const nodes = [];

  Object.entries(runData).forEach(([nodeName, nodeRuns]) => {
    nodeRuns.forEach((nodeRun, runIndex) => {
      const { inputData, outputData } = extractIO(nodeRun);

      nodes.push({
        nodeName,
        nodeType: nodeTypeLookup[nodeName],
        startTimeMs: nodeRun.startTime,
        durationMs: nodeRun.executionTime,
        status: nodeRun.executionStatus,
        executionIndex: nodeRun.executionIndex,
        runIndex,
        sourceNode: nodeRun.source?.[0]?.previousNode || null,
        inputData,
        outputData,
      });
    });
  });

  nodes.sort((a, b) => {
    const aIsAgent = a.nodeName.toLowerCase().includes("agent") || (a.nodeType && a.nodeType.includes("agent"));
    const bIsAgent = b.nodeName.toLowerCase().includes("agent") || (b.nodeType && b.nodeType.includes("agent"));

    const aIsSub = a.nodeName.includes("Model") || a.nodeName.includes("Vector") || a.nodeName.includes("Embeddings");
    const bIsSub = b.nodeName.includes("Model") || b.nodeName.includes("Vector") || b.nodeName.includes("Embeddings");

    if (aIsAgent && bIsSub) return -1;
    if (bIsAgent && aIsSub) return 1;

    return (a.startTimeMs || 0) - (b.startTimeMs || 0) || (a.executionIndex || 0) - (b.executionIndex || 0);
  });

  return { parent, nodes };
}

// ── Span construction avec attribution des icônes ─────────────────────────────
function buildAndExportSpans(parsed, traceId) {
  const { parent, nodes } = parsed;

  const startMs = Date.parse(parent.startedAt);
  const endMs = Date.parse(parent.stoppedAt);

  forcedTraceId = traceId;
  const rootSpan = tracer.startSpan(
    `n8n.${parent.workflowName}`,
    { startTime: startMs }
  );

  rootSpan.setAttribute("n8n.execution.id", parent.executionId);
  rootSpan.setAttribute("n8n.execution.status", parent.status);
  rootSpan.setAttribute("n8n.execution.mode", parent.mode);
  rootSpan.setAttribute("n8n.workflow.id", parent.workflowId);
  rootSpan.setAttribute("n8n.workflow.name", parent.workflowName);
  if (parent.retryOf) rootSpan.setAttribute("n8n.execution.retry_of", String(parent.retryOf));
  if (parent.errorMessage) rootSpan.setAttribute("n8n.execution.error.message", parent.errorMessage);
  if (parent.errorNode) rootSpan.setAttribute("n8n.execution.error.node", parent.errorNode);
  rootSpan.setStatus({ code: toOtelStatusCode(parent.status) });

  const rootCtx = trace.setSpan(context.active(), rootSpan);
  const spanContextMap = new Map();

  nodes.forEach(node => {
    const durationMs = Math.max(node.durationMs || 1, 1);
    let nodeStartMs = node.startTimeMs || startMs;
    let nodeEndMs = nodeStartMs + durationMs;

    if (nodeStartMs < startMs) nodeStartMs = startMs;
    if (nodeEndMs > endMs) nodeEndMs = endMs;
    if (nodeEndMs <= nodeStartMs) nodeEndMs = nodeStartMs + 10;

    let parentSpanContext = null;
    const nameLower = node.nodeName.toLowerCase();
    const typeLower = (node.nodeType || "").toLowerCase();

    const isEmbeddings = nameLower.includes("embedding") || typeLower.includes("embeddings");
    const isVectorStore = nameLower.includes("vector") || typeLower.includes("vectorstore") || nameLower.includes("pinecone");
    const isLlmModel = nameLower.includes("model") || typeLower.includes("lm");
    const isAgent = nameLower.includes("agent") || typeLower.includes("agent");

    const agentEntry = Array.from(spanContextMap.entries()).find(([name]) => name.toLowerCase().includes("agent"));

    if (isEmbeddings) {
      const vectorEntry = Array.from(spanContextMap.entries()).find(([name]) =>
        name.toLowerCase().includes("vector") || name.toLowerCase().includes("pinecone")
      );
      if (vectorEntry) {
        parentSpanContext = vectorEntry[1];
      } else if (agentEntry) {
        parentSpanContext = agentEntry[1];
      }
    } else if (isLlmModel || isVectorStore) {
      if (agentEntry) {
        parentSpanContext = agentEntry[1];
      }
    }

    if (!parentSpanContext && node.sourceNode && spanContextMap.has(node.sourceNode)) {
      parentSpanContext = spanContextMap.get(node.sourceNode);
    }

    const parentCtxToUse = parentSpanContext
      ? trace.setSpan(context.active(), trace.wrapSpanContext(parentSpanContext))
      : rootCtx;

    if (isLlmModel) {
      forcedNextSpanId = deriveSpanId(traceId + `-chat-${node.runIndex}`);
    } else if (isEmbeddings) {
      forcedNextSpanId = deriveSpanId(traceId + `-embeddings-${node.runIndex}`);
    }

    const childSpan = tracer.startSpan(
      node.nodeName,
      { startTime: nodeStartMs },
      parentCtxToUse
    );

    // 🔹 ATTRIBUTS POUR LES ICÔNES DANS LANGFUSE
    if (isLlmModel) {
      childSpan.setAttribute("openinference.type", "LLM");
      childSpan.setAttribute("gen_ai.system", "google");
      childSpan.setAttribute("gen_ai.request.model", "gemini-3.5-flash");
    } else if (isVectorStore || isEmbeddings) {
      childSpan.setAttribute("openinference.type", "RETRIEVER");
      childSpan.setAttribute("db.system", "pinecone");
    } else if (isAgent) {
      childSpan.setAttribute("openinference.type", "AGENT");
    } else {
      childSpan.setAttribute("openinference.type", "CHAIN");
    }

    childSpan.setAttribute("n8n.node.name", node.nodeName);
    childSpan.setAttribute("n8n.node.type", node.nodeType || "unknown");
    childSpan.setAttribute("n8n.node.status", node.status || "unknown");
    childSpan.setAttribute("n8n.node.duration_ms", durationMs);
    childSpan.setAttribute("n8n.node.execution_index", node.executionIndex || 0);
    childSpan.setAttribute("n8n.node.run_index", node.runIndex || 0);
    if (node.sourceNode) childSpan.setAttribute("n8n.node.source", node.sourceNode);

    if (node.inputData) {
      childSpan.setAttribute("input.value", truncate(node.inputData));
    }
    if (node.outputData) {
      childSpan.setAttribute("output.value", truncate(node.outputData));
    }

    childSpan.setStatus({ code: toOtelStatusCode(node.status) });

    spanContextMap.set(node.nodeName, childSpan.spanContext());

    childSpan.end(nodeEndMs);
  });

  rootSpan.end(endMs);

  console.log(`[otel] Exportation réussie pour [${parent.executionId}] trace_id=${traceId}`);
}

// ── Public function ──────────────────────────────────────────────────────────
async function processTraceparentAsync(traceparentHeader) {
  try {
    const parts = traceparentHeader.split("-");
    if (parts.length < 4) {
      console.warn(`[otel] traceparent malformé: ${traceparentHeader}`);
      return;
    }
    const traceId = parts[1];

    const executionId = await resolveExecutionIdFromTraceId(traceId);
    if (!executionId) return;

    let detail = null;
    for (let attempt = 0; attempt < 10; attempt++) {
      detail = await n8nGet(`/executions/${executionId}?includeData=true`);
      if (detail && detail.stoppedAt) break;
      await new Promise(r => setTimeout(r, 2000));
    }

    if (!detail || !detail.stoppedAt) {
      console.warn(`[otel] Exécution ${executionId} non terminée, abandon.`);
      return;
    }

    const parsed = parseExecution(detail);
    buildAndExportSpans(parsed, traceId);

  } catch (e) {
    console.error(`[otel] Erreur lors du traitement du traceparent ${traceparentHeader}:`, e.message);
  }
}

const processedTraceIds = new Set();
const PROCESSED_TTL_MS = 5 * 60 * 1000;

function triggerTraceFromTraceparent(traceparentHeader) {
  if (!traceparentHeader) return;
  const parts = traceparentHeader.split("-");
  if (parts.length < 2) return;
  const traceId = parts[1];

  if (processedTraceIds.has(traceId)) return;
  processedTraceIds.add(traceId);
  setTimeout(() => processedTraceIds.delete(traceId), PROCESSED_TTL_MS);

  processTraceparentAsync(traceparentHeader);
}

module.exports = { setupTracer, triggerTraceFromTraceparent };
