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
    # If state_path is already set, return finished
    if hasattr(request.app.state, "state_path"):
        gcs_uri = getattr(request.app.state, "state_path")
        state_timestamp = getattr(request.app.state, "state_path_timestamp", None)
        now = datetime.datetime.now(datetime.timezone.utc)
        if state_timestamp and (now - state_timestamp).total_seconds() > 600:
            print(f"[login-status] state_path found but is older than 10 minutes: {gcs_uri} (timestamp: {state_timestamp})")
            delattr(request.app.state, "state_path")
            if hasattr(request.app.state, "state_path_timestamp"):
                delattr(request.app.state, "state_path_timestamp")
            return {"status": "none", "ok": False}
        print(f"[login-status] state_path found in app state: {gcs_uri}")
        return {"status": "finished", "ok": True}

    print("[login-status] state_path not found, searching GCS for recent state file...")
    # Example GCS URI: gs://bucket_name/path/to/state.json
    # You may want to hardcode or configure the bucket/prefix as needed
    BUCKET_NAME = "insta-state"  # Set to your actual bucket
    EXT = ".json"                      # Only look for .json state files
    now = datetime.datetime.now(datetime.timezone.utc)
    ten_minutes_ago = now - datetime.timedelta(minutes=10)

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    # List blobs with the prefix
    blobs = list(bucket.list_blobs())
    print(f"[login-status] Listing all blobs:")
    for b in blobs:
        print(f"  - {b.name} (updated: {b.updated}, endswith .json: {b.name.endswith(EXT)})")
    print(f"[login-status] Checking which blobs are newer than {ten_minutes_ago.isoformat()}: ")
    for b in blobs:
        if b.name.endswith(EXT):
            print(f"  - {b.name}: updated {b.updated}, is_newer: {b.updated and b.updated > ten_minutes_ago}")
    print(f"[login-status] Found {len(blobs)} blobs in bucket '{BUCKET_NAME}'")
    # Filter for .json files newer than 10 minutes
    recent_blobs = [b for b in blobs if b.name.endswith(EXT) and b.updated and b.updated > ten_minutes_ago]
    print(f"[login-status] Found {len(recent_blobs)} recent .json blobs newer than 10 minutes")
    if recent_blobs:
        recent_blobs.sort(key=lambda b: b.updated, reverse=True)
        blob = recent_blobs[0]
        gcs_uri = f"gs://{BUCKET_NAME}/{blob.name}"
        print(f"[login-status] Using most recent blob: {gcs_uri} (updated: {blob.updated})")
        request.app.state.state_path = gcs_uri
        request.app.state.state_path_timestamp = blob.updated
        return {"status": "finished", "ok": True}
    print("[login-status] No recent state file found in GCS, returning none")
    return {"status": "none", "ok": False}
    
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
