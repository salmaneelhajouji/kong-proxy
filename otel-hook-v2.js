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
// Plage de recherche pour la résolution inversée trace_id -> execution_id
const REVERSE_SEARCH_RANGE = parseInt(process.env.REVERSE_SEARCH_RANGE || "300", 10);

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
// IMPORTANT : formule IDENTIQUE à celle utilisée côté n8n (nœud Code in
// JavaScript) pour générer le traceparent envoyé à Kong.
function deriveTraceId(seed) {
  return crypto.createHash("sha256").update(String(seed)).digest("hex").slice(0, 32);
}

function deriveSpanId(seed) {
  return crypto.createHash("sha256").update(String(seed) + "-span").digest("hex").slice(0, 16);
}

// ── Reverse lookup: trace_id -> execution_id ─────────────────────────────────
// On reçoit un traceparent (donc un trace_id) via le header custom, mais on a
// besoin de execution_id (en clair) pour interroger l'API n8n. Comme le hash
// n'est pas réversible mathématiquement, on retrouve execution_id en testant
// une plage de valeurs récentes et en comparant leur hash au trace_id reçu.
//
// Optimisation : on garde en cache le dernier execution_id connu (via l'API
// n8n) pour ne tester qu'une petite fenêtre autour de cette valeur, au lieu
// de rebalayer une plage arbitraire à chaque appel.
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
  // Rafraîchit le curseur "dernier execution_id connu" si on ne l'a pas encore,
  // ou périodiquement pour rester à jour.
  if (lastKnownExecutionId === null) {
    try {
      lastKnownExecutionId = await fetchLatestExecutionId();
    } catch (e) {
      console.error(`[otel] Impossible de récupérer le dernier execution_id:`, e.message);
      return null;
    }
  }

  // Teste une fenêtre de recherche autour du dernier ID connu, en partant du
  // plus récent vers le plus ancien (l'exécution en cours est probablement
  // très récente, donc on la trouve vite).
  const upperBound = lastKnownExecutionId + 20; // marge de sécurité si de nouvelles exécutions sont apparues entre-temps
  const lowerBound = Math.max(1, lastKnownExecutionId - REVERSE_SEARCH_RANGE);

  for (let candidate = upperBound; candidate >= lowerBound; candidate--) {
    if (deriveTraceId(candidate) === traceId) {
      lastKnownExecutionId = Math.max(lastKnownExecutionId, candidate); // met à jour le curseur
      return candidate;
    }
  }

  console.warn(`[otel] Aucun execution_id trouvé pour trace_id=${traceId} dans la plage [${lowerBound}, ${upperBound}]`);
  return null;
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
function buildAndExportSpans(parsed, traceId) {
  const { parent, nodes } = parsed;
  const parentSpanId = deriveSpanId(parent.executionId);

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

// ── Public function: process one traceparent ─────────────────────────────────
async function processTraceparentAsync(traceparentHeader) {
  try {
    // Extrait le trace_id du header traceparent : "00-{trace_id}-{span_id}-01"
    const parts = traceparentHeader.split("-");
    if (parts.length < 4) {
      console.warn(`[otel] traceparent malformé: ${traceparentHeader}`);
      return;
    }
    const traceId = parts[1];

    const executionId = await resolveExecutionIdFromTraceId(traceId);
    if (!executionId) return; // déjà loggé dans resolveExecutionIdFromTraceId

    // Attend que l'exécution soit terminée (stoppedAt renseigné)
    let detail = null;
    for (let attempt = 0; attempt < 10; attempt++) {
      detail = await n8nGet(`/executions/${executionId}?includeData=true`);
      if (detail.stoppedAt) break;
      await new Promise(r => setTimeout(r, 3000));
    }

    if (!detail || !detail.stoppedAt) {
      console.warn(`[otel] Exécution ${executionId} pas encore terminée après plusieurs tentatives, abandon.`);
      return;
    }

    const parsed = parseExecution(detail);
    buildAndExportSpans(parsed, traceId);

  } catch (e) {
    console.error(`[otel] Erreur lors du traitement du traceparent ${traceparentHeader}:`, e.message);
  }
}

// ── Dédoublonnage : évite de traiter deux fois le même trace_id
// si plusieurs appels LLM de la même exécution arrivent avec le même traceparent
const processedTraceIds = new Set();
const PROCESSED_TTL_MS = 5 * 60 * 1000;

function triggerTraceFromTraceparent(traceparentHeader) {
  if (!traceparentHeader) return;
  if (processedTraceIds.has(traceparentHeader)) {
    return;
  }
  processedTraceIds.add(traceparentHeader);
  setTimeout(() => processedTraceIds.delete(traceparentHeader), PROCESSED_TTL_MS);

  // Fire-and-forget : ne bloque jamais la réponse HTTP vers Kong/n8n
  processTraceparentAsync(traceparentHeader);
}

module.exports = { setupTracer, triggerTraceFromTraceparent };
