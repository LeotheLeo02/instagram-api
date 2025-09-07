from __future__ import annotations
import traceback, json
from pathlib   import Path
from urllib.parse import unquote

from fastapi      import FastAPI, HTTPException, status, Query, Request
from google.cloud.run_v2.types import execution as execution_plus
from google.cloud.logging_v2.services.logging_service_v2 import (
    LoggingServiceV2Client,
)
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import run_v2
from google.cloud import logging_v2
from google.cloud import storage
import datetime
import os
import logging

from pydantic     import BaseModel, Field

from scraper import (      # new
    run_remote_scrape        # new
)

app = FastAPI(title="IG helper API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173", 
        "http://127.0.0.1:5173", 
        "http://localhost:1420",  # Tauri dev server
        "tauri://localhost", 
        "tauri://127.0.0.1"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RegisterStateRequest(BaseModel):
    gcs_uri: str
# ────────────────────────────────────────────────────────────────────────────
# 2)  /remote-scrape  — use saved cookies, call Railway API
# ────────────────────────────────────────────────────────────────────────────
class RemoteReq(BaseModel):
    target     : str = Field(..., example="utmartin")
    target_yes : int = 12
    batch_size : int = 40
    num_bio_pages : int = 4
    # New: allow per-job criteria override, either by preset id (for auditing) or raw text
    criteria_preset_id: str | None = None
    criteria_text: str | None = None
    api_url    : str = (
        "https://scrape-orchestrator-mhnsdh4esa-ew.a.run.app/run-scrape"
    )

class RemoteResp(BaseModel):
    count   : int
    results : list[dict]

@app.post("/register-state")
async def register_state(req: RegisterStateRequest, request: Request):
    """Persist the GCS URI produced by the desktop login flow."""
    request.app.state.state_path = req.gcs_uri
    request.app.state.state_path_timestamp = datetime.datetime.now(datetime.timezone.utc)
    return {"ok": True}

@app.get("/login-status")
async def login_status(request: Request):
    """Ultra-efficient login status check with automatic cleanup.

    Cookie freshness is controlled via env variables:
      - LOGIN_STATE_FRESH_SECONDS (default: 86400 seconds = 24h)
      - LOGIN_STATE_CLEANUP_SECONDS (default: 2592000 seconds = 30d)
    """

    def _get_int_env(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)))
        except Exception:
            return default

    FRESH_SECONDS = _get_int_env("LOGIN_STATE_FRESH_SECONDS", 86400)  # 24h
    CLEANUP_SECONDS = _get_int_env("LOGIN_STATE_CLEANUP_SECONDS", 2592000)  # 30d

    # Fast path: Check in-memory state first
    if hasattr(request.app.state, "state_path"):
        gcs_uri = getattr(request.app.state, "state_path")
        state_timestamp = getattr(request.app.state, "state_path_timestamp", None)

        if state_timestamp:
            now = datetime.datetime.now(datetime.timezone.utc)
            age_seconds = (now - state_timestamp).total_seconds()

            # If state is fresh (< FRESH_SECONDS), return immediately
            if age_seconds < FRESH_SECONDS:
                return {"status": "finished", "ok": True}

            # State is stale, clear it
            delattr(request.app.state, "state_path")
            delattr(request.app.state, "state_path_timestamp")

    # Ultra-efficient GCS search with parallel processing
    BUCKET_NAME = "insta-state"
    now = datetime.datetime.now(datetime.timezone.utc)
    fresh_threshold = now - datetime.timedelta(seconds=FRESH_SECONDS)
    cleanup_threshold = now - datetime.timedelta(seconds=CLEANUP_SECONDS)

    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)

        # Single API call to get all blobs with minimal data
        blobs = list(bucket.list_blobs(fields="items(name,updated)"))

        # Ultra-fast filtering with list comprehensions
        json_blobs = [b for b in blobs if b.name.endswith('.json') and b.updated]

        if not json_blobs:
            return {"status": "none", "ok": False}

        # Find the most recent blobs in one pass
        best_blob = None  # updated within FRESH_SECONDS
        newest_blob = None  # newest overall
        old_blobs = []

        for blob in json_blobs:
            # Track newest overall
            if newest_blob is None or blob.updated > newest_blob.updated:
                newest_blob = blob

            # Fresh (under FRESH_SECONDS)
            if blob.updated > fresh_threshold:
                if best_blob is None or blob.updated > best_blob.updated:
                    best_blob = blob
            # Older than cleanup threshold (30d) -> schedule cleanup
            elif blob.updated < cleanup_threshold:
                old_blobs.append(blob)

        # Async cleanup of old files (non-blocking)
        if old_blobs:
            import asyncio
            asyncio.create_task(_cleanup_old_files(old_blobs))

        # Return result immediately if fresh (< FRESH_SECONDS)
        if best_blob:
            gcs_uri = f"gs://{BUCKET_NAME}/{best_blob.name}"
            request.app.state.state_path = gcs_uri
            request.app.state.state_path_timestamp = best_blob.updated
            return {"status": "finished", "ok": True}

        # If not fresh, but there is a state within the last 30 days, still treat as valid
        if newest_blob and newest_blob.updated >= cleanup_threshold:
            gcs_uri = f"gs://{BUCKET_NAME}/{newest_blob.name}"
            request.app.state.state_path = gcs_uri
            request.app.state.state_path_timestamp = newest_blob.updated
            return {"status": "finished", "ok": True}

        # Only return invalid if the newest is older than 30 days (or none exist)
        return {"status": "none", "ok": False}

    except Exception as e:
        print(f"[login-status] Error: {e}")
        return {"status": "none", "ok": False}


async def _cleanup_old_files(old_blobs):
    """Background cleanup of old files - non-blocking."""
    try:
        deleted_count = 0
        for blob in old_blobs:
            try:
                blob.delete()
                deleted_count += 1
            except:
                pass  # Ignore deletion errors in background task
        
        if deleted_count > 0:
            print(f"[login-status] Background cleanup: deleted {deleted_count} old files")
    except:
        pass  # Fail silently for background cleanup
    
@app.post("/remote-scrape")
async def remote_scrape(body: RemoteReq, request: Request):
    """
    Trigger a remote scrape using the gs:// cookie URI previously
    registered via /register-state.
    """
    gs_uri = getattr(request.app.state, "state_path", None)

    if not (isinstance(gs_uri, str) and gs_uri.startswith("gs://")):
        raise HTTPException(400, "No cookie gs:// URI registered – call /register-state first")

    try:
        # Generate epoch_id if not provided
        epoch_id = f"op_{int(datetime.datetime.now(datetime.timezone.utc).timestamp())}"

        # Pre-create START artifacts so polling never 404s
        try:
            bucket_name = os.environ.get("SCREENSHOT_BUCKET") or os.environ.get("RESULTS_BUCKET")
            if not bucket_name:
                raise RuntimeError("Results bucket not configured")
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            prefix = f"scrapes/{body.target}/{epoch_id}/"
            start_blob = bucket.blob(prefix + "START")
            start_blob.upload_from_string(json.dumps({"ok": True, "ts": int(datetime.datetime.now(datetime.timezone.utc).timestamp())}), content_type="application/json")
            meta_blob = bucket.blob(prefix + "meta.json")
            running_meta = {
                "status": "running",
                "target": body.target,
                "target_yes": body.target_yes,
                "yes_count": 0,
                "operation_id": epoch_id,
            }
            # Attach criteria selection onto meta for transparency (no backend global changes)
            if body.criteria_preset_id is not None:
                running_meta["criteria_preset_id"] = body.criteria_preset_id
            if body.criteria_text is not None:
                running_meta["criteria_text"] = body.criteria_text
            meta_blob.upload_from_string(json.dumps(running_meta), content_type="application/json")
            print(f"🔧 [DEBUG] remote_scrape meta: preset_id={running_meta.get('criteria_preset_id')} text_len={len(running_meta.get('criteria_text','') or '')}")
        except Exception as _e:
            # Non-fatal; continue
            pass

        resp = await run_remote_scrape(
            state_gcs_uri = gs_uri,
            target        = body.target,
            api_url       = body.api_url,
            target_yes    = body.target_yes,
            batch_size    = body.batch_size,
            num_bio_pages = body.num_bio_pages,
            exec_id       = epoch_id,
            criteria_preset_id = body.criteria_preset_id,
            criteria_text = body.criteria_text,
        )

        # -------------------- response handling --------------------
        if resp.get("status") == "queued":              # LRO path
            op_id = resp.get("execution") or resp.get("name")
            if not op_id:
                raise HTTPException(500, "operation id missing in response")
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={"status": "queued", "operation": op_id, "exec_id": epoch_id},
            )

        # synchronous completion
        results = resp["results"]
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"status": "completed",
                     "count": len(results),
                     "results": results},
        )

    except Exception as e:
        raise HTTPException(500, str(e))



# ────────────────────────────────────────────────────────────────────────────
# GCS-based status lookup (new artifact scheme)
# ────────────────────────────────────────────────────────────────────────────

def _get_int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _gcs_status_lookup(target: str, exec_id: str):
    """Return status by checking GCS artifacts written to GCS.

    Layout:
      scrapes/<target>/<exec_id>/results.json
      scrapes/<target>/<exec_id>/meta.json
      scrapes/<target>/<exec_id>/START (uploaded first)
      scrapes/<target>/<exec_id>/DONE  (uploaded last)

    Behavior:
      - Wait (up to slack) for START to appear; if still missing, 404
      - If DONE not present yet -> status: running
      - If DONE present but results.json not yet readable -> status: running
      - If DONE present and results.json readable -> status: completed
    """
    bucket_name = os.environ.get("SCREENSHOT_BUCKET") or os.environ.get("RESULTS_BUCKET")
    if not bucket_name:
        raise HTTPException(500, "Results bucket not configured: set SCREENSHOT_BUCKET or RESULTS_BUCKET")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Slack window/polling
    slack_seconds = _get_int_env("SCRAPE_STATUS_SLACK_SECONDS", 120)
    read_retries = _get_int_env("SCRAPE_STATUS_READ_RETRIES", 3)
    read_retry_delay_ms = _get_int_env("SCRAPE_STATUS_READ_RETRY_DELAY_MS", 300)

    prefix = f"scrapes/{target}/{exec_id}/"

    # 1) Wait for START to appear (folder existence)
    start_blob = bucket.blob(prefix + "START")
    if not start_blob.exists(client):
        try:
            import time as _time
            deadline = _time.monotonic() + max(0, slack_seconds)
            while _time.monotonic() < deadline:
                _time.sleep(max(0.05, read_retry_delay_ms / 1000.0))
                if start_blob.exists(client):
                    break
        except Exception:
            pass
        if not start_blob.exists(client):
            print(f"DBG START not found for target={target}, exec_id={exec_id}")
            raise HTTPException(404, f"Scrape artifacts not found for target={target}, exec_id={exec_id}")

    # 2) If DONE not yet present → running
    done_blob = bucket.blob(prefix + "DONE")
    if not done_blob.exists(client):
        # Try to enrich running status with current meta, if present
        meta_blob = bucket.blob(prefix + "meta.json")
        try:
            if meta_blob.exists(client):
                meta_text = meta_blob.download_as_text()
                meta = json.loads(meta_text)
                # Ensure status is running for consistency until DONE appears
                meta["status"] = "running"
                return meta
        except Exception:
            pass
        return {"status": "running"}

    # Results with small retry to handle GCS write propagation
    results_blob = bucket.blob(prefix + "results.json")
    if not results_blob.exists(client):
        # Not yet visible; report running to allow slack
        # Try to include meta if available
        try:
            meta_blob = bucket.blob(prefix + "meta.json")
            if meta_blob.exists(client):
                meta_text = meta_blob.download_as_text()
                meta = json.loads(meta_text)
                meta["status"] = "running"
                return meta
        except Exception:
            pass
        return {"status": "running"}

    last_exc = None
    for _ in range(max(1, read_retries)):
        try:
            results_text = results_blob.download_as_text()
            results = json.loads(results_text)
            break
        except Exception as e:
            last_exc = e
            # brief sleep before retry
            try:
                import time as _time
                _time.sleep(read_retry_delay_ms / 1000.0)
            except Exception:
                pass
    else:
        # All retries failed; treat as running to avoid transient 500s
        try:
            meta_blob = bucket.blob(prefix + "meta.json")
            if meta_blob.exists(client):
                meta_text = meta_blob.download_as_text()
                meta = json.loads(meta_text)
                meta["status"] = "running"
                return meta
        except Exception:
            pass
        return {"status": "running"}

    # Include summary from meta if available
    try:
        meta_blob = bucket.blob(prefix + "meta.json")
        meta = {}
        if meta_blob.exists(client):
            meta_text = meta_blob.download_as_text()
            meta = json.loads(meta_text)
    except Exception:
        meta = {}
    return {"status": "completed", "results": results, **({k: v for k, v in meta.items() if k not in {"status", "results"}})}

JOB_NAME = "scrape-job"             # << change if your job has another name


@app.get("/scrape-status")
async def scrape_status(
    target: str = Query(...),
    exec_id: str = Query(...),
):
    """Check scrape status using GCS artifacts.

    Args:
        target: Target Instagram username
        exec_id: Operation ID (like op_<epoch>)

    Example:
        /scrape-status?target=utmartin&exec_id=op_1722980000
    """
    return _gcs_status_lookup(target=target, exec_id=exec_id)


@app.get("/legacy-scrape-status")
async def legacy_scrape_status(
    operation: str = Query(...),
):
    """Legacy endpoint for checking scrape status using Cloud Run Operations.

    Args:
        operation: Cloud Run Operations path

    Example:
        /legacy-scrape-status?operation=/operations/abc-123
    """
    op_path = unquote(operation)

    if "/operations/" not in op_path:
        raise HTTPException(400, "Expecting an /operations/ path")

    ops_client = run_v2.ExecutionsClient().transport.operations_client
    try:
        op = ops_client.get_operation(op_path)
    except Exception as e:
        raise HTTPException(400, f"Could not retrieve operation: {e}")

    if not op.done:
        return {"status": "running"}

    if op.error.code:
        return {"status": "failed", "message": op.error.message}

    try:
        # Get execution path from operation response
        exec_pb = execution_plus.Execution()._pb
        op.response.Unpack(exec_pb)
        exec_path = exec_pb.name

        # Fetch the Execution (now guaranteed to exist & be finished)
        exec_client = run_v2.ExecutionsClient()
        execution = exec_client.get_execution(name=exec_path)

        # Pull JSON array from Cloud Logging (legacy)
        project_id = execution.name.split("/")[1]
        exec_short = execution.name.split("/")[-1]

        # Relaxed filter (no severity constraint)
        log_filter = (
            'resource.type="cloud_run_job" '
            f'labels."run.googleapis.com/execution_name"="{exec_short}"'
        )

        log_cli = LoggingServiceV2Client()
        entries = log_cli.list_log_entries(
            request={
                "resource_names": [f"projects/{project_id}"],
                "filter": log_filter,
                "order_by": "timestamp asc",
                "page_size": 1000,
            }
        )

        buffer, collecting = [], False
        results = None

        for entry in entries:
            payload = (
                getattr(entry, "text_payload", "")
                or getattr(entry, "json_payload", {}).get("message", "")
            )

            # Start collecting when we hit the opening "["
            if "[" in payload and not collecting:
                collecting = True
            if collecting:
                buffer.append(payload.rstrip("\n"))
            if collecting and "]" in payload:  # Reached the closing "]"
                try:
                    results = json.loads("\n".join(buffer))
                except json.JSONDecodeError:
                    # reset and keep scanning
                    collecting = False
                    buffer = []
                    continue
                break

        if results is None:
            raise HTTPException(500, "Job completed but results not found in logs")

    except Exception:
        print("🔴 scrape_status (legacy logs) failed\n", traceback.format_exc())
        raise HTTPException(500, "Error processing execution results from logs")

    return {"status": "completed", "results": results}


# ────────────────────────────────────────────────────────────────────────────
# Deletion of GCS artifacts after client persists locally
# ────────────────────────────────────────────────────────────────────────────
@app.delete("/scrape-artifacts")
async def delete_scrape_artifacts(
    target: str = Query(...),
    exec_id: str = Query(...),
):
    """Delete all artifacts under scrapes/<target>/<exec_id>/ in the results bucket.

    This is intended to be called by the desktop app once results have been
    saved locally, to avoid leaving artifacts in GCS.
    """
    bucket_name = os.environ.get("SCREENSHOT_BUCKET") or os.environ.get("RESULTS_BUCKET")
    if not bucket_name:
        raise HTTPException(500, "Results bucket not configured: set SCREENSHOT_BUCKET or RESULTS_BUCKET")

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        prefix = f"scrapes/{target}/{exec_id}/"

        blobs = list(bucket.list_blobs(prefix=prefix))
        if not blobs:
            raise HTTPException(404, f"No artifacts found for prefix {prefix}")

        deleted = 0
        errors: list[dict] = []
        names: list[str] = [b.name for b in blobs]
        for blob in blobs:
            try:
                blob.delete()
                deleted += 1
            except Exception as e:
                logging.warning(f"Failed to delete blob {blob.name}: {e}")
                errors.append({"name": blob.name, "error": str(e)})

        if deleted == 0 and errors:
            raise HTTPException(500, f"Failed to delete any artifacts under {prefix}: {errors[:3]}")

        return {"ok": True, "deleted": deleted, "failed": len(errors), "items": names, "prefix": prefix}
    except Exception as e:
        raise HTTPException(500, f"Failed to delete artifacts: {e}")
