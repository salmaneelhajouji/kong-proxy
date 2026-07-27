import os
import logging
import hashlib
from datetime import datetime, timezone

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
# from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import StatusCode, NonRecordingSpan, SpanContext, TraceFlags

log = logging.getLogger(__name__)

# ── Tracer (module-level, initialised once via setup_tracer()) ─────────────────
_tracer: trace.Tracer | None = None

# ── Deterministic trace/span ID derivation ─────────────────────────────────────
# IMPORTANT: cette formule DOIT être identique à celle utilisée côté n8n
# (dans le nœud "Code in JavaScript") pour que Kong et n8n partagent le même
# trace_id. Ne PAS modifier l'un sans l'autre.
def derive_trace_id(seed: str) -> int:
    """Dérive un trace_id (128 bits) de façon déterministe depuis un seed."""
    h = hashlib.sha256(str(seed).encode()).hexdigest()
    return int(h[:32], 16)  # 32 hex chars = 128 bits

def derive_span_id(seed: str) -> int:
    """Dérive un span_id (64 bits) de façon déterministe depuis un seed."""
    h = hashlib.sha256((str(seed) + "-span").encode()).hexdigest()
    return int(h[:16], 16)  # 16 hex chars = 64 bits

# ── Setup ──────────────────────────────────────────────────────────────────────
def parse_headers(raw: str) -> dict:
    """Parse 'key1=val1,key2=val2' into {'key1': 'val1', 'key2': 'val2'}"""
    result = {}
    if not raw.strip():
        return result
    for pair in raw.split(","):
        pair = pair.strip()
        if "=" in pair:
            k, v = pair.split("=", 1)
            result[k.strip()] = v.strip()
    return result

def parse_resource_attributes(raw: str) -> dict:
    """Parse optional extra resource attributes from env."""
    return parse_headers(raw)

def setup_tracer():
    """
    Initialise the OTEL tracer provider and exporter from environment variables.
    Must be called once before export_execution().
    """
    global _tracer

    otel_endpoint    = os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"]
    otel_headers     = parse_headers(os.getenv("OTEL_EXPORTER_OTLP_HEADERS", ""))
    service_name     = os.getenv("OTEL_SERVICE_NAME", "n8n-cloud")
    extra_attrs      = parse_resource_attributes(os.getenv("OTEL_RESOURCE_ATTRIBUTES", ""))

    resource = Resource.create({
        "service.name": service_name,
        **extra_attrs,
    })

    exporter = OTLPSpanExporter(
        endpoint=otel_endpoint,
        headers=otel_headers,
    )

    provider = TracerProvider(resource=resource)
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    _tracer = trace.get_tracer("n8n-poller")

    log.info(f"OTEL tracer initialised → {otel_endpoint} (service: {service_name})")

# ── Time helpers ───────────────────────────────────────────────────────────────
def iso_to_ns(iso_string: str) -> int:
    """Convert ISO 8601 string to Unix nanoseconds."""
    dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000_000)

def ms_to_ns(ms: int) -> int:
    """Convert Unix milliseconds to Unix nanoseconds."""
    return ms * 1_000_000

# ── Status mapping ─────────────────────────────────────────────────────────────
def to_otel_status(n8n_status: str) -> StatusCode:
    """Map n8n execution status to OTEL StatusCode."""
    return {
        "success":  StatusCode.OK,
        "error":    StatusCode.ERROR,
        "canceled": StatusCode.UNSET,
    }.get(n8n_status, StatusCode.UNSET)

# ── Span builders ──────────────────────────────────────────────────────────────
def _build_parent_span(parsed: dict) -> trace.Span:
    parent   = parsed["parent"]
    start_ns = iso_to_ns(parent["started_at"])
    end_ns   = iso_to_ns(parent["stopped_at"])

    # ✅ NOUVEAU : construire un contexte de span "fantôme" avec le trace_id
    # dérivé de execution_id. Ce même execution_id doit être utilisé côté
    # n8n (nœud Code) pour générer le traceparent envoyé à Kong.
    seed = parent["execution_id"]
    custom_trace_id = derive_trace_id(seed)
    custom_span_id = derive_span_id(seed)

    fake_parent_context = trace.set_span_in_context(
        NonRecordingSpan(
            SpanContext(
                trace_id=custom_trace_id,
                span_id=custom_span_id,
                is_remote=True,
                trace_flags=TraceFlags(TraceFlags.SAMPLED),
            )
        )
    )

    span = _tracer.start_span(
        name=f"n8n.{parent['workflow_name']}",
        start_time=start_ns,
        context=fake_parent_context,
    )

    # ── Attributes ────────────────────────────────────────────────────────────
    span.set_attribute("n8n.execution.id",     parent["execution_id"])
    span.set_attribute("n8n.execution.status", parent["status"])
    span.set_attribute("n8n.execution.mode",   parent["mode"])
    span.set_attribute("n8n.workflow.id",      parent["workflow_id"])
    span.set_attribute("n8n.workflow.name",    parent["workflow_name"])

    if parent["retry_of"] is not None:
        span.set_attribute("n8n.execution.retry_of", str(parent["retry_of"]))

    if parent["error_message"] is not None:
        span.set_attribute("n8n.execution.error.message", parent["error_message"])

    if parent["error_node"] is not None:
        span.set_attribute("n8n.execution.error.node", parent["error_node"])

    # ── Status ────────────────────────────────────────────────────────────────
    span.set_status(to_otel_status(parent["status"]))

    return span, end_ns

def _build_child_spans(parsed: dict, parent_span: trace.Span, parent_end_ns: int):
    parent_ctx      = trace.set_span_in_context(parent_span)
    parent_start_ns = iso_to_ns(parsed["parent"]["started_at"])

    stale_detected = False
    last_end_ns    = parent_start_ns

    for node in parsed["nodes"]:
        node_duration_ns = ms_to_ns(node["duration_ms"] or 1)
        node_start_ns    = ms_to_ns(node["start_time_ms"])

        if node_start_ns < last_end_ns:
            stale_detected = True

        if stale_detected:
            node_start_ns = last_end_ns

        node_end_ns = node_start_ns + node_duration_ns
        node_end_ns = min(node_end_ns, parent_end_ns)
        last_end_ns = node_end_ns

        # ✅ Utilise le NOM DU NOEUD directement comme nom de span (au lieu
        # de laisser "node.execute" générique) — résout aussi le problème
        # de lisibilité de l'arbre identifié plus tôt
        span = _tracer.start_span(
            name=node["node_name"],
            start_time=node_start_ns,
            context=parent_ctx,
        )

        span.set_attribute("n8n.node.name",            node["node_name"])
        span.set_attribute("n8n.node.type",            node["node_type"] or "unknown")
        span.set_attribute("n8n.node.status",          node["status"] or "unknown")
        span.set_attribute("n8n.node.duration_ms",     node["duration_ms"] or 0)
        span.set_attribute("n8n.node.execution_index", node["execution_index"] or 0)
        span.set_attribute("n8n.node.run_index",       node["run_index"] or 0)

        if node["source_node"] is not None:
            span.set_attribute("n8n.node.source", node["source_node"])

        span.set_status(to_otel_status(node["status"]))
        span.end(end_time=node_end_ns)

# ── Public export function ─────────────────────────────────────────────────────
def export_execution(parsed: dict):
    """
    Build and export OTEL spans for a single parsed execution.
    Child spans are ended first, then the parent span.
    """
    if _tracer is None:
        raise RuntimeError("Tracer not initialised — call setup_tracer() first")

    parent      = parsed["parent"]
    exec_id     = parent["execution_id"]
    workflow    = parent["workflow_name"]
    status      = parent["status"]

    try:
        parent_span, parent_end_ns = _build_parent_span(parsed)
        _build_child_spans(parsed, parent_span, parent_end_ns)
        parent_span.end(end_time=parent_end_ns)

        log.info(f"  ✓ Exported trace [{exec_id}] n8n.{workflow} — {status} ({len(parsed['nodes'])} node spans), trace_id derived from execution_id")

    except Exception as e:
        log.error(f"  ✗ Failed to export trace for execution {exec_id}: {e}")
        raise