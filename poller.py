import os
import time
import logging
import requests
from dotenv import load_dotenv
from otel import setup_tracer, export_execution

load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
N8N_HOST              = os.environ["N8N_HOST"].rstrip("/")
N8N_API_KEY           = os.environ["N8N_API_KEY"]
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 60))
CURSOR_FILE           = os.getenv("CURSOR_FILE", "last_execution_id.txt")
MAX_RETRIES           = int(os.getenv("MAX_RETRIES", 5))
STATUSES              = ["success", "error", "canceled"]

# ── Cursor persistence ─────────────────────────────────────────────────────────
def load_last_id() -> str | None:
    try:
        value = open(CURSOR_FILE).read().strip()
        return value if value else None
    except FileNotFoundError:
        return None

def save_last_id(execution_id: str):
    dir_path = os.path.dirname(CURSOR_FILE)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    with open(CURSOR_FILE, "w") as f:
        f.write(execution_id)

# ── n8n API ────────────────────────────────────────────────────────────────────
def n8n_get(path: str, params: dict = None) -> dict:
    url = f"{N8N_HOST}/api/v1{path}"
    resp = requests.get(
        url,
        headers={"X-N8N-API-KEY": N8N_API_KEY, "Accept": "application/json"},
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

def fetch_new_execution_ids(last_id: str | None) -> list[str]:
    """
    Fetch IDs of all new completed executions since last_id.
    Paginates using nextCursor, stops early once we hit an already-seen ID.
    Returns IDs sorted ascending (oldest first).
    """
    new_ids = []

    for status in STATUSES:
        cursor = None

        while True:
            params = {"status": status, "limit": 100}
            if cursor:
                params["cursor"] = cursor

            body = n8n_get("/executions", params=params)
            page = body.get("data", [])

            done = False
            for execution in page:
                exec_id = str(execution["id"])

                if last_id and int(exec_id) <= int(last_id):
                    done = True
                    break

                new_ids.append(exec_id)

            if done or not body.get("nextCursor"):
                break

            cursor = body["nextCursor"]

    # Sort ascending so we process oldest → newest
    new_ids.sort(key=lambda x: int(x))
    return new_ids

def fetch_execution_detail(execution_id: str) -> dict:
    """Fetch full execution data including per-node run data."""
    return n8n_get(f"/executions/{execution_id}", params={"includeData": "true"})

# ── Parse execution into OTEL-ready structure ──────────────────────────────────
def parse_execution(detail: dict) -> dict:
    """
    Extract all OTEL-relevant data from a raw execution detail response.
    Returns a clean dict with parent span data and a list of child span data.
    """

    # ── Build node type lookup from workflowData.nodes ────────────────────────
    # { node_name -> node_type }
    node_type_lookup = {
        node["name"]: node["type"]
        for node in detail.get("workflowData", {}).get("nodes", [])
    }

    # ── Parent span data (overall workflow execution) ──────────────────────────
    result_data = detail.get("data", {}).get("resultData", {})
    error       = result_data.get("error")

    parent = {
        # Identity
        "execution_id":   str(detail.get("id")),
        "workflow_id":    detail.get("workflowId"),
        "workflow_name":  detail.get("workflowData", {}).get("name"),

        # Timing (ISO strings — convert to ns when building spans)
        "started_at":     detail.get("startedAt"),
        "stopped_at":     detail.get("stoppedAt"),

        # Execution metadata
        "status":         detail.get("status"),
        "mode":           detail.get("mode"),         # manual, trigger, webhook, etc.
        "retry_of":       detail.get("retryOf"),      # ID of original if this is a retry

        # Error (only set on error executions)
        "error_message":  error.get("message") if isinstance(error, dict) else None,
        "error_node":     error.get("node", {}).get("name") if isinstance(error, dict) else None,
    }

    # ── Child span data (one per node run) ────────────────────────────────────
    run_data = result_data.get("runData", {})
    nodes    = []

    for node_name, node_runs in run_data.items():
        for run_index, node_run in enumerate(node_runs):
            nodes.append({
                # Identity
                "node_name":       node_name,
                "node_type":       node_type_lookup.get(node_name),

                # Timing (ms — convert to ns when building spans)
                "start_time_ms":   node_run.get("startTime"),
                "duration_ms":     node_run.get("executionTime"),

                # Execution metadata
                "status":          node_run.get("executionStatus"),
                "execution_index": node_run.get("executionIndex"),
                "run_index":       run_index,   # which iteration (for looped nodes)

                # Source — which node triggered this one
                "source_node":     node_run.get("source", [{}])[0].get("previousNode") if node_run.get("source") else None,
            })

    # Sort nodes by execution_index so they appear in order
    nodes.sort(key=lambda n: (n["execution_index"] or 0, n["run_index"]))

    return {"parent": parent, "nodes": nodes}

# ── Display parsed execution ───────────────────────────────────────────────────
def display_parsed_execution(parsed: dict):
    parent = parsed["parent"]
    nodes  = parsed["nodes"]

    log.info(f"")
    log.info(f"  ┌─ EXECUTION {parent['execution_id']} {'─' * 30}")
    log.info(f"  │  workflow_name:  {parent['workflow_name']}")
    log.info(f"  │  workflow_id:    {parent['workflow_id']}")
    log.info(f"  │  status:         {parent['status']}")
    log.info(f"  │  mode:           {parent['mode']}")
    log.info(f"  │  started_at:     {parent['started_at']}")
    log.info(f"  │  stopped_at:     {parent['stopped_at']}")
    log.info(f"  │  retry_of:       {parent['retry_of']}")
    if parent["error_message"]:
        log.info(f"  │  error_message:  {parent['error_message']}")
        log.info(f"  │  error_node:     {parent['error_node']}")
    log.info(f"  │")

    for node in nodes:
        log.info(f"  ├─ NODE: {node['node_name']}")
        log.info(f"  │    node_type:       {node['node_type']}")
        log.info(f"  │    status:          {node['status']}")
        log.info(f"  │    start_time_ms:   {node['start_time_ms']}")
        log.info(f"  │    duration_ms:     {node['duration_ms']}")
        log.info(f"  │    execution_index: {node['execution_index']}")
        log.info(f"  │    run_index:       {node['run_index']}")
        log.info(f"  │    source_node:     {node['source_node']}")
        log.info(f"  │")

    log.info(f"  └─{'─' * 44}")
    log.info(f"")

# ── Poll ───────────────────────────────────────────────────────────────────────
def poll():
    last_id = load_last_id()
    log.info(f"Polling — last_id={last_id or 'none'}")

    try:
        new_ids = fetch_new_execution_ids(last_id)
    except Exception as e:
        log.error(f"Failed to fetch executions: {e}")
        return

    if not new_ids:
        log.info("No new executions.")
        return

    log.info(f"Found {len(new_ids)} new execution(s): {new_ids}")

    for exec_id in new_ids:
        retry_count = 0
        success     = False

        while retry_count < MAX_RETRIES and not success:
            try:
                detail = fetch_execution_detail(exec_id)
                parsed = parse_execution(detail)
                display_parsed_execution(parsed)
                export_execution(parsed)
                success = True

            except Exception as e:
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    log.warning(f"  Failed to fetch detail for execution {exec_id} (attempt {retry_count}/{MAX_RETRIES}): {e}")
                    time.sleep(1)
                else:
                    log.error(f"  Failed to fetch detail for execution {exec_id} after {MAX_RETRIES} attempts: {e}")
                    log.error(f"  Skipping execution {exec_id} and moving on")

        # Save cursor whether successful or skipped after max retries
        save_last_id(exec_id)
        log.info(f"  Cursor saved → {exec_id}")

        # Small delay to avoid hammering the API
        time.sleep(0.1)

# ── Main loop ──────────────────────────────────────────────────────────────────
def main():
    log.info("─────────────────────────────────────")
    log.info("  n8n Execution Poller")
    log.info("─────────────────────────────────────")
    log.info(f"  Host:     {N8N_HOST}")
    log.info(f"  Interval: {POLL_INTERVAL_SECONDS}s")
    log.info(f"  Statuses: {', '.join(STATUSES)}")
    log.info("─────────────────────────────────────")

    setup_tracer()

    while True:
        try:
            poll()
        except Exception as e:
            log.error(f"Unexpected error: {e}")

        log.info(f"Sleeping {POLL_INTERVAL_SECONDS}s ...")
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()