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
    batch_size : int = 30
    num_bio_pages : int = 3
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
    """Ultra-efficient login status check with automatic cleanup."""
    
    # Fast path: Check in-memory state first
    if hasattr(request.app.state, "state_path"):
        gcs_uri = getattr(request.app.state, "state_path")
        state_timestamp = getattr(request.app.state, "state_path_timestamp", None)
        
        if state_timestamp:
            now = datetime.datetime.now(datetime.timezone.utc)
            age_seconds = (now - state_timestamp).total_seconds()
            
            # If state is fresh (< 10 minutes), return immediately
            if age_seconds < 600:
                return {"status": "finished", "ok": True}
            
            # State is stale, clear it
            delattr(request.app.state, "state_path")
            delattr(request.app.state, "state_path_timestamp")
    
    # Ultra-efficient GCS search with parallel processing
    BUCKET_NAME = "insta-state"
    now = datetime.datetime.now(datetime.timezone.utc)
    ten_minutes_ago = now - datetime.timedelta(minutes=10)
    one_hour_ago = now - datetime.timedelta(hours=1)
    
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        
        # Single API call to get all blobs with minimal data
        blobs = list(bucket.list_blobs(fields="items(name,updated)"))
        
        # Ultra-fast filtering with list comprehensions
        json_blobs = [b for b in blobs if b.name.endswith('.json') and b.updated]
        
        if not json_blobs:
            return {"status": "none", "ok": False}
        
        # Find the most recent valid blob in one pass
        best_blob = None
        old_blobs = []
        
        for blob in json_blobs:
            if blob.updated > ten_minutes_ago:
                if best_blob is None or blob.updated > best_blob.updated:
                    best_blob = blob
            elif blob.updated < one_hour_ago:
                old_blobs.append(blob)
        
        # Async cleanup of old files (non-blocking)
        if old_blobs:
            import asyncio
            asyncio.create_task(_cleanup_old_files(old_blobs))
        
        # Return result immediately if found
        if best_blob:
            gcs_uri = f"gs://{BUCKET_NAME}/{best_blob.name}"
            request.app.state.state_path = gcs_uri
            request.app.state.state_path_timestamp = best_blob.updated
            return {"status": "finished", "ok": True}
        
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
        resp = await run_remote_scrape(
            state_gcs_uri = gs_uri,
            target        = body.target,
            api_url       = body.api_url,
            target_yes    = body.target_yes,
            batch_size    = body.batch_size,
            num_bio_pages = body.num_bio_pages
        )

        # -------------------- response handling --------------------
        if resp.get("status") == "queued":              # LRO path
            op_id = resp.get("execution") or resp.get("name")
            if not op_id:
                raise HTTPException(500, "operation id missing in response")
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={"status": "queued", "operation": op_id},
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


JOB_NAME = "scrape-job"             # << change if your job has another name

@app.get("/scrape-status")
async def scrape_status(
    operation: str = Query(
        ...,
        description="Cloud Run LRO path  e.g. "
        "projects/<p>/locations/<r>/operations/<uuid>",
    )
):
    op_path = unquote(operation)    # undo %2F from the client

    # ──────────────────────────────
    # 1️⃣  Poll the long-running OP
    # ──────────────────────────────
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
        # ──────────────────────────────
        # 2️⃣  Get execution path from operation response
        # ──────────────────────────────
        exec_pb = execution_plus.Execution()._pb
        op.response.Unpack(exec_pb)
        exec_path = exec_pb.name

        # Fetch the Execution (now guaranteed to exist & be finished)
        exec_client = run_v2.ExecutionsClient()
        execution = exec_client.get_execution(name=exec_path)

        # ──────────────────────────────
        # 3️⃣  Pull your JSON array from Cloud Logging
        # ──────────────────────────────
        project_id = execution.name.split("/")[1]
        exec_short = execution.name.split("/")[-1]           # scrape-job-<uuid>

        log_filter = (
            'resource.type="cloud_run_job" '
            f'labels."run.googleapis.com/execution_name"="{exec_short}" '
            'severity=DEFAULT'
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
        print("Entries: ", entries)

        buffer, collecting = [], False
        results = None

        for entry in entries:
            payload = (
                getattr(entry, "text_payload", "")
                or getattr(entry, "json_payload", {}).get("message", "")
            )

            # start collecting when we hit the opening "["
            if "[" in payload and not collecting:
                collecting = True
            if collecting:
                buffer.append(payload.rstrip("\n"))
            if collecting and "]" in payload:     # reached the closing "]"
                try:
                    results = json.loads("\n".join(buffer))
                except json.JSONDecodeError:
                    pass
                break

        if results is None:
            raise HTTPException(
                500, "Job completed but results not found in logs"
            )

    except Exception as e:
        print("🔴 scrape_status failed\n", traceback.format_exc())
        raise HTTPException(500, f"Error processing execution results: {str(e)}")

    return {"status": "completed", "results": results}
