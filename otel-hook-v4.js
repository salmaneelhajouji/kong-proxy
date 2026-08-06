// otel-hook-v4.js
const crypto = require("crypto");
const { trace, context } = require("@opentelemetry/api");
const { BasicTracerProvider, SimpleSpanProcessor } = require("@opentelemetry/sdk-trace-node");
const { OTLPTraceExporter } = require("@opentelemetry/exporter-trace-otlp-http");
const { Resource } = require("@opentelemetry/resources");
const { SemanticResourceAttributes } = require("@opentelemetry/semantic-conventions");

const N8N_HOST = process.env.N8N_HOST;
const N8N_API_KEY = process.env.N8N_API_KEY;
const OTEL_ENDPOINT = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
const OTEL_HEADERS_RAW = process.env.OTEL_EXPORTER_OTLP_HEADERS || "";
const PUBLIC_PROXY_URL = process.env.RENDER_EXTERNAL_URL || "https://kong-proxy.onrender.com";
const REVERSE_SEARCH_RANGE = parseInt(process.env.REVERSE_SEARCH_RANGE || "300", 10);
const MAX_ATTR_LENGTH = parseInt(process.env.MAX_ATTR_LENGTH || "8000", 10);

const MCP_SERVER_WORKFLOW_ID = "BMxUfKVV5C0rvQzo";

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
let provider = null;

function setupTracer() {
  const resource = new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: "n8n-agentic4api-webhook",
  });

  const exporter = new OTLPTraceExporter({
    url: OTEL_ENDPOINT,
    headers: parseHeaders(OTEL_HEADERS_RAW),
  });

  provider = new BasicTracerProvider({
    resource,
    idGenerator: customIdGenerator,
  });

  provider.addSpanProcessor(new SimpleSpanProcessor(exporter));
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
  try {
    const latest = await fetchLatestExecutionId();
    if (latest) lastKnownExecutionId = latest;
  } catch (e) {}

  if (lastKnownExecutionId) {
    const upperBound = lastKnownExecutionId + 20;
    const lowerBound = Math.max(1, lastKnownExecutionId - REVERSE_SEARCH_RANGE);

    for (let candidate = upperBound; candidate >= lowerBound; candidate--) {
      if (deriveTraceId(candidate) === traceId) {
        lastKnownExecutionId = Math.max(lastKnownExecutionId, candidate);
        return candidate;
      }
    }
  }

  try {
    const url = `${N8N_HOST.replace(/\/$/, "")}/api/v1/executions?limit=10`;
    const resp = await fetch(url, {
      headers: { "X-N8N-API-KEY": N8N_API_KEY, "Accept": "application/json" }
    });
    if (resp.ok) {
      const body = await resp.json();
      const executions = body.data || [];
      const parentExec = executions.find(e => e.workflowId !== MCP_SERVER_WORKFLOW_ID) || executions[0];
      if (parentExec) return parseInt(parentExec.id, 10);
    }
  } catch (e) {}

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
  if (!resp.ok) throw new Error(`n8n API error ${resp.status}: ${await resp.text()}`);
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
        nodeName: nodeName.trim(),
        nodeType: nodeTypeLookup[nodeName],
        startTimeMs: nodeRun.startTime,
        durationMs: nodeRun.executionTime,
        status: nodeRun.executionStatus,
        executionIndex: nodeRun.executionIndex,
        runIndex,
        sourceNode: nodeRun.source?.[0]?.previousNode || null,
        inputData,
        outputData,
        executionId: String(detail.id),
        workflowName: detail.workflowData?.name || "unknown-workflow"
      });
    });
  });

  return { parent, nodes };
}

async function waitForExecutionToFinish(executionId, maxAttempts = 40) {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const detail = await n8nGet(`/executions/${executionId}?includeData=true`);
      if (detail && detail.stoppedAt) return detail;
    } catch (e) {}
    await new Promise(r => setTimeout(r, 1200));
  }
  return null;
}

function getNodeRank(nodeName) {
  const name = (nodeName || "").trim();
  if (name === "AI Agent") return 1;
  if (name === "LLM" || name === "MCP Client1") return 2;
  if (name === "MCP Server Trigger") return 3;
  if (name === "Pinecone Vector Store") return 4;
  if (name === "Embeddings OpenAI") return 5;
  return 0;
}

async function buildAndExportUnifiedSpans(allParsed, traceId) {
  const mainParsed = allParsed[0];
  const { parent: mainParent } = mainParsed;

  const allNodes = [];
  allParsed.forEach(parsed => {
    allNodes.push(...parsed.nodes);
  });

  allNodes.sort((a, b) => {
    const rankA = getNodeRank(a.nodeName);
    const rankB = getNodeRank(b.nodeName);
    if (rankA !== rankB) return rankA - rankB;
    return (a.startTimeMs || 0) - (b.startTimeMs || 0);
  });

  const startMs = Date.parse(mainParent.startedAt);
  let maxEndMs = Date.parse(mainParent.stoppedAt);

  allNodes.forEach(n => {
    const end = (n.startTimeMs || startMs) + Math.max(n.durationMs || 1, 1);
    if (end > maxEndMs) maxEndMs = end;
  });

  forcedTraceId = traceId;
  forcedNextSpanId = deriveSpanId(`${mainParent.executionId}_root`);
  
  const rootSpan = tracer.startSpan(
    `n8n.${mainParent.workflowName}`,
    { startTime: startMs }
  );

  rootSpan.setAttribute("n8n.execution.id", mainParent.executionId);
  rootSpan.setAttribute("n8n.execution.status", mainParent.status);
  rootSpan.setAttribute("n8n.execution.mode", mainParent.mode);
  rootSpan.setAttribute("n8n.workflow.id", mainParent.workflowId);
  rootSpan.setAttribute("n8n.workflow.name", mainParent.workflowName);
  rootSpan.setStatus({ code: toOtelStatusCode(mainParent.status) });

  const rootCtx = trace.setSpan(context.active(), rootSpan);
  const spanContextMap = new Map();

  allNodes.forEach(node => {
    const nodeName = node.nodeName;
    const runIndex = node.runIndex || 0;
    const durationMs = Math.max(node.durationMs || 1, 1);
    let nodeStartMs = node.startTimeMs || startMs;
    let nodeEndMs = nodeStartMs + durationMs;

    if (nodeStartMs < startMs) nodeStartMs = startMs;
    if (nodeEndMs <= nodeStartMs) nodeEndMs = nodeStartMs + 10;

    forcedNextSpanId = deriveSpanId(`${node.executionId}_node_${nodeName}_${runIndex}`);

    let parentCtxToUse = rootCtx;

    if (nodeName === "LLM" || nodeName === "MCP Client1") {
      const agentCtx = spanContextMap.get("AI Agent_0") || spanContextMap.get("AI Agent");
      if (agentCtx) parentCtxToUse = trace.setSpan(context.active(), trace.wrapSpanContext(agentCtx));
    } else if (nodeName === "MCP Server Trigger") {
      const mcpClientCtx = spanContextMap.get(`MCP Client1_${runIndex}`) || spanContextMap.get("MCP Client1_0") || spanContextMap.get("MCP Client1");
      if (mcpClientCtx) parentCtxToUse = trace.setSpan(context.active(), trace.wrapSpanContext(mcpClientCtx));
      else {
        const agentCtx = spanContextMap.get("AI Agent_0") || spanContextMap.get("AI Agent");
        if (agentCtx) parentCtxToUse = trace.setSpan(context.active(), trace.wrapSpanContext(agentCtx));
      }
    } else if (nodeName === "Pinecone Vector Store") {
      const triggerCtx = spanContextMap.get(`MCP Server Trigger_${runIndex}`) || spanContextMap.get("MCP Server Trigger_0") || spanContextMap.get("MCP Server Trigger");
      if (triggerCtx) parentCtxToUse = trace.setSpan(context.active(), trace.wrapSpanContext(triggerCtx));
    } else if (nodeName === "Embeddings OpenAI") {
      const vectorCtx = spanContextMap.get(`Pinecone Vector Store_${runIndex}`) || spanContextMap.get("Pinecone Vector Store_0") || spanContextMap.get("Pinecone Vector Store");
      if (vectorCtx) parentCtxToUse = trace.setSpan(context.active(), trace.wrapSpanContext(vectorCtx));
    }

    const childSpan = tracer.startSpan(
      nodeName,
      { startTime: nodeStartMs },
      parentCtxToUse
    );

    if (nodeName === "LLM") {
      childSpan.setAttribute("openinference.type", "LLM");
      childSpan.setAttribute("gen_ai.system", "google");
    } else if (nodeName === "Pinecone Vector Store" || nodeName === "Embeddings OpenAI") {
      childSpan.setAttribute("openinference.type", "RETRIEVER");
    } else if (nodeName === "AI Agent") {
      childSpan.setAttribute("openinference.type", "AGENT");
    } else {
      childSpan.setAttribute("openinference.type", "CHAIN");
    }

    childSpan.setAttribute("n8n.execution.id", node.executionId);
    childSpan.setAttribute("n8n.workflow.name", node.workflowName);
    childSpan.setAttribute("n8n.node.name", nodeName);
    childSpan.setAttribute("n8n.node.run_index", runIndex);
    childSpan.setAttribute("n8n.node.type", node.nodeType || "unknown");
    childSpan.setAttribute("n8n.node.duration_ms", durationMs);

    if (node.inputData) childSpan.setAttribute("input.value", truncate(node.inputData));
    if (node.outputData) childSpan.setAttribute("output.value", truncate(node.outputData));

    childSpan.setStatus({ code: toOtelStatusCode(node.status) });

    spanContextMap.set(`${nodeName}_${runIndex}`, childSpan.spanContext());
    spanContextMap.set(nodeName, childSpan.spanContext());

    childSpan.end(nodeEndMs);
  });

  rootSpan.end(maxEndMs);

  if (provider) await provider.forceFlush();
  console.log(`[otel] 🚀 Succès : Arbre parfait exporté sous [Exécution #${mainParent.executionId}] | trace_id=${traceId}`);
}

async function processTraceparentAsync(traceparentHeader) {
  try {
    const parts = traceparentHeader.split("-");
    if (parts.length < 4) return;
    const traceId = parts[1];

    const parentExecutionId = await resolveExecutionIdFromTraceId(traceId);
    if (!parentExecutionId) return;

    console.log(`[otel] Attente active de l'exécution parent #${parentExecutionId}...`);

    const renderKeepAliveTimer = setInterval(() => {
      fetch(`${PUBLIC_PROXY_URL}/`).catch(() => {});
    }, 2000);

    let parentDetail = null;
    try {
      parentDetail = await waitForExecutionToFinish(parentExecutionId, 40);
    } finally {
      clearInterval(renderKeepAliveTimer);
    }

    if (!parentDetail || !parentDetail.stoppedAt) {
      console.warn(`[otel] Exécution parent #${parentExecutionId} non terminée, abandon.`);
      return;
    }

    const parentStartMs = Date.parse(parentDetail.startedAt);
    const parentEndMs = Date.parse(parentDetail.stoppedAt);
    const allParsed = [parseExecution(parentDetail)];

    for (let offset = 1; offset <= 3; offset++) {
      const childId = parentExecutionId + offset;
      try {
        const childDetail = await waitForExecutionToFinish(childId, 5);
        if (childDetail && childDetail.startedAt) {
          const childStartMs = Date.parse(childDetail.startedAt);
          if (childStartMs >= parentStartMs - 1000 && childStartMs <= parentEndMs + 5000) {
            console.log(`[otel] Sous-workflow #${childId} (${childDetail.workflowData?.name}) rattaché à l'arbre`);
            allParsed.push(parseExecution(childDetail));
          }
        }
      } catch (e) {}
    }

    await buildAndExportUnifiedSpans(allParsed, traceId);

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
