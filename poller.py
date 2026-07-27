import os
import time
import logging
import threading
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
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
PORT                  = int(os.getenv("PORT", 10000))  # Render fournit PORT automatiquement

# ── Cursor persistence ─────────────────────────────────────────────────────────
# ⚠️ Sur Render Web Service (disque non persistant entre redéploiements),
# ce fichier survit tant que le service ne redémarre pas, mais sera perdu
# à chaque redéploiement/redémarrage. Pour une persistance à toute épreuve,
# il faudrait un stockage externe (Render Key Value, Postgres, etc.) —
# acceptable pour un premier déploiement, à améliorer si besoin plus tard.
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

    new_ids.sort(key=lambda x: int(x))
    return new_ids

def fetch_execution_detail(execution_id: str) -> dict:
    return n8n_get(f"/executions/{execution_id}", params={"includeData": "true"})

# ── Parse execution into OTEL-ready structure ──────────────────────────────────
def parse_execution(detail: dict) -> dict:
    node_type_lookup = {
        node["name"]: node["type"]
        for node in detail.get("workflowData", {}).get("nodes", [])
    }

    result_data = detail.get("data", {}).get("resultData", {})
    error       = result_data.get("error")

    parent = {
        "execution_id":   str(detail.get("id")),
        "workflow_id":    detail.get("workflowId"),
        "workflow_name":  detail.get("workflowData", {}).get("name"),
        "started_at":     detail.get("startedAt"),
        "stopped_at":     detail.get("stoppedAt"),
        "status":         detail.get("status"),
        "mode":           detail.get("mode"),
        "retry_of":       detail.get("retryOf"),
        "error_message":  error.get("message") if isinstance(error, dict) else None,
        "error_node":     error.get("node", {}).get("name") if isinstance(error, dict) else None,
    }

    run_data = result_data.get("runData", {})
    nodes    = []

    for node_name, node_runs in run_data.items():
        for run_index, node_run in enumerate(node_runs):
            nodes.append({
                "node_name":       node_name,
                "node_type":       node_type_lookup.get(node_name),
                "start_time_ms":   node_run.get("startTime"),
                "duration_ms":     node_run.get("executionTime"),
                "status":          node_run.get("executionStatus"),
                "execution_index": node_run.get("executionIndex"),
                "run_index":       run_index,
                "source_node":     node_run.get("source", [{}])[0].get("previousNode") if node_run.get("source") else None,
            })

    nodes.sort(key=lambda n: (n["execution_index"] or 0, n["run_index"]))

    return {"parent": parent, "nodes": nodes}

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
                export_execution(parsed)
                success = True

            except Exception as e:
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    log.warning(f"  Failed to process execution {exec_id} (attempt {retry_count}/{MAX_RETRIES}): {e}")
                    time.sleep(1)
                else:
                    log.error(f"  Failed to process execution {exec_id} after {MAX_RETRIES} attempts: {e}")

        save_last_id(exec_id)
        log.info(f"  Cursor saved → {exec_id}")
        time.sleep(0.1)

# ── Background polling loop (runs in a separate thread) ────────────────────────
def polling_loop():
    setup_tracer()
    while True:
        try:
            poll()
        except Exception as e:
            log.error(f"Unexpected error: {e}")
        log.info(f"Sleeping {POLL_INTERVAL_SECONDS}s ...")
        time.sleep(POLL_INTERVAL_SECONDS)

# ── Minimal HTTP server (just to satisfy Render's "Web Service" requirement) ───
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "ok", "service": "n8n-otel-poller"}')

    def log_message(self, format, *args):
        pass  # Silence les logs HTTP par défaut pour ne pas polluer la console

def main():
    log.info("─────────────────────────────────────")
    log.info("  n8n Execution Poller (web service mode)")
    log.info("─────────────────────────────────────")
    log.info(f"  Host:     {N8N_HOST}")
    log.info(f"  Interval: {POLL_INTERVAL_SECONDS}s")
    log.info(f"  HTTP port: {PORT}")
    log.info("─────────────────────────────────────")

    # Démarre la boucle de polling dans un thread séparé, en arrière-plan
    polling_thread = threading.Thread(target=polling_loop, daemon=True)
    polling_thread.start()

    # Le thread principal écoute juste un port HTTP minimal,
    # pour que Render considère le service comme "actif"
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    log.info(f"Health check server listening on port {PORT}")
    server.serve_forever()

if __name__ == "__main__":
    main()
