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

let tracer = null;

function setupTracer() {
  const resource = new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: "n8n-agentic4api-webhook",
  });

  const exporter = new OTLPTraceExporter({
    url: OTEL_ENDPOINT,
    headers: parseHeaders(OTEL_HEADERS_RAW),
  });

  const provider = new BasicTracerProvider({ resource });
  provider.addSpanProcessor(new BatchSpanProcessor(exporter));
  provider.register();

  tracer = trace.getTracer("n8n-webhook-hook");
  console.log(`[otel] Tracer initialisé -> ${OTEL_ENDPOINT}`);
}

// ── Trace/span ID derivation ─────────────────────────────────────────────────
// IMPORTANT : formule IDENTIQUE côté Kong (via traceparent reçu) — le trace_id
// dérivé ici DOIT correspondre à celui déjà utilisé par les spans Kong pour
// cette même exécution.
function deriveTraceId(seed) {
  return crypto.createHash("sha256").update(String(seed)).digest("hex").slice(0, 32);
}

function deriveSpanId(seed) {
  return crypto.createHash("sha256").update(String(seed) + "-span").digest("hex").slice(0, 16);
}

// ── n8n API helpers ──────────────────────────────────────────────────────────
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
  // 0 = UNSET, 1 = OK, 2 = ERROR (valeurs numériques SpanStatusCode)
  if (n8nStatus === "success") return 1;
  if (n8nStatus === "error") return 2;
  return 0;
}

// ── Parse execution detail into OTEL-ready structure ────────────────────────
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
      nodes.push({
        nodeName,
        nodeType: nodeTypeLookup[nodeName],
        startTimeMs: nodeRun.startTime,
        durationMs: nodeRun.executionTime,
        status: nodeRun.executionStatus,
        executionIndex: nodeRun.executionIndex,
        runIndex,
        sourceNode: nodeRun.source?.[0]?.previousNode || null,
      });
    });
  });

  nodes.sort((a, b) => (a.executionIndex || 0) - (b.executionIndex || 0) || a.runIndex - b.runIndex);

  return { parent, nodes };
}

// ── Span construction with forced trace_id/span_id ──────────────────────────
function buildAndExportSpans(parsed) {
  const { parent, nodes } = parsed;
  const seed = parent.executionId;

  const traceId = deriveTraceId(seed);
  const parentSpanId = deriveSpanId(seed);

  const fakeSpanContext = {
    traceId,
    spanId: parentSpanId,
    traceFlags: 1,
    isRemote: true,
  };

  const fakeParentSpan = trace.wrapSpanContext(fakeSpanContext);
  const parentCtx = trace.setSpan(context.active(), fakeParentSpan);

  const startMs = Date.parse(parent.startedAt);
  const endMs = Date.parse(parent.stoppedAt);

  const rootSpan = tracer.startSpan(
    `n8n.${parent.workflowName}`,
    { startTime: startMs },
    parentCtx
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

  let lastEndMs = startMs;
  let staleDetected = false;

  nodes.forEach(node => {
    const durationMs = node.durationMs || 1;
    let nodeStartMs = node.startTimeMs;

    if (nodeStartMs < lastEndMs) staleDetected = true;
    if (staleDetected) nodeStartMs = lastEndMs;

    let nodeEndMs = nodeStartMs + durationMs;
    if (nodeEndMs > endMs) nodeEndMs = endMs;
    lastEndMs = nodeEndMs;

    const childSpan = tracer.startSpan(
      node.nodeName,
      { startTime: nodeStartMs },
      rootCtx
    );

    childSpan.setAttribute("n8n.node.name", node.nodeName);
    childSpan.setAttribute("n8n.node.type", node.nodeType || "unknown");
    childSpan.setAttribute("n8n.node.status", node.status || "unknown");
    childSpan.setAttribute("n8n.node.duration_ms", node.durationMs || 0);
    childSpan.setAttribute("n8n.node.execution_index", node.executionIndex || 0);
    childSpan.setAttribute("n8n.node.run_index", node.runIndex || 0);
    if (node.sourceNode) childSpan.setAttribute("n8n.node.source", node.sourceNode);
    childSpan.setStatus({ code: toOtelStatusCode(node.status) });

    childSpan.end(nodeEndMs);
  });

  rootSpan.end(endMs);

  console.log(`[otel] Exported trace [${parent.executionId}] n8n.${parent.workflowName} - ${parent.status} (${nodes.length} node spans), trace_id=${traceId}`);
}

// ── Public function: process one execution by ID ────────────────────────────
// Appelée de façon FIRE-AND-FORGET (asynchrone, sans bloquer la réponse à Kong)
async function processExecutionAsync(executionId) {
  try {
    // Petit délai pour laisser le temps à n8n de finaliser l'écriture des
    // données d'exécution avant qu'on ne les récupère via l'API — l'exécution
    // est probablement encore en cours au moment où ce header arrive
    // (le premier appel LLM se produit AU MILIEU de l'exécution, pas à la fin).
    // On retente plusieurs fois si l'exécution n'est pas encore "stoppedAt".
    let detail = null;
    for (let attempt = 0; attempt < 10; attempt++) {
      detail = await n8nGet(`/executions/${executionId}?includeData=true`);
      if (detail.stoppedAt) break;
      await new Promise(r => setTimeout(r, 3000)); // attend 3s avant de réessayer
    }

    if (!detail || !detail.stoppedAt) {
      console.warn(`[otel] Exécution ${executionId} pas encore terminée après plusieurs tentatives, abandon.`);
      return;
    }

    const parsed = parseExecution(detail);
    buildAndExportSpans(parsed);

  } catch (e) {
    console.error(`[otel] Erreur lors du traitement de l'exécution ${executionId}:`, e.message);
  }
}

// ── Dédoublonnage simple : évite de traiter deux fois la même exécution
// si plusieurs appels LLM (chat + embeddings) de la même exécution arrivent
// avec le même x-n8n-execution-id
const processedExecutions = new Set();
const PROCESSED_TTL_MS = 5 * 60 * 1000; // 5 minutes

function triggerExecutionTrace(executionId) {
  if (!executionId) return;
  if (processedExecutions.has(executionId)) {
    return; // déjà traité (ou en cours de traitement) pour cette exécution
  }
  processedExecutions.add(executionId);
  setTimeout(() => processedExecutions.delete(executionId), PROCESSED_TTL_MS);

  // Fire-and-forget : ne bloque jamais la réponse HTTP vers Kong/n8n
  processExecutionAsync(executionId);
}

module.exports = { setupTracer, triggerExecutionTrace };
