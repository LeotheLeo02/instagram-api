# app/scraper.py
"""
Two-step helper for local + remote workflow
------------------------------------------
1) interactive_login(...)  → let the user log in to Instagram once and
   save the Playwright storage state to disk.
2) run_remote_scrape(...)  → read that state file, b64-encode it, POST it
   to your Railway FastAPI service, and return the results.
"""

import base64, json, os, asyncio, tempfile
from pathlib import Path
from typing import List, Dict

import httpx
from playwright.async_api import async_playwright, Browser, Page


################################################################################
# 1.  Log in manually & save cookies
################################################################################

async def interactive_login(
    state_path: Path = Path("state.json"),
) -> None:
    """
    Launches a *visible* Instagram window.
    You log in, then press Enter in the terminal → cookies are persisted.
    """
    async with async_playwright() as pw:
        browser: Browser = await pw.chromium.launch(headless=False)
        page: Page = await browser.new_page()
        await page.goto("https://www.instagram.com/")

        input("👉  Log in inside the window, then press <Enter> here to save state... ")

        await page.context.storage_state(path=state_path)
        await browser.close()
        print(f"✅  Saved state to {state_path.absolute()}")


################################################################################
# 2.  Send that state to the Railway scraper
################################################################################

async def run_remote_scrape(
    *,
    state_gcs_uri: str,
    target: str,
    api_url: str = "https://scrape-orchestrator-mhnsdh4esa-ew.a.run.app/run-scrape",
    target_yes: int = 12,
    batch_size: int = 30,
) -> Dict:
    """
    Invoke the /run-scrape endpoint, passing a gs:// cookie-state URI.
    """
    if not state_gcs_uri.startswith("gs://"):
        raise ValueError("state_gcs_uri must start with 'gs://'")

    payload = {
        "target": target,
        "target_yes": target_yes,
        "batch_size": batch_size,
        "state_gcs_uri": state_gcs_uri,   # ← only field we send now
    }

    timeout = httpx.Timeout(900.0, connect=60.0, read=900.0,
                            write=60.0, pool=60.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(api_url, json=payload)
        r.raise_for_status()
        data = r.json()

    if "results" in data:                      # finished sync
        print(f"✅ Remote scrape returned {len(data['results'])} yes-profiles.")
        return {"results": data["results"]}
    if data.get("status") == "queued":         # queued async
        print(f"✅ Job queued – execution: {data.get('execution') or data.get('name')}")
        return data
    raise RuntimeError(f"Remote scrape failed: {data}")


################################################################################
# 3.  Convenience CLI:  python -m app.scraper login / scrape
################################################################################

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="IG login & remote scrape helper")
    sub = parser.add_subparsers(dest="cmd", required=True)

    #  interactive_login
    p_login = sub.add_parser("login", help="Open IG and save cookies")
    p_login.add_argument("--state", default="state.json", help="Where to save Playwright state")

    #  run_remote_scrape
    p_scrape = sub.add_parser("scrape", help="Call Railway /scrape with saved state")
    p_scrape.add_argument("target", help="Instagram account to analyse (followers are scanned)")
    p_scrape.add_argument("--state",      default="state.json", help="state file from login step")
    p_scrape.add_argument("--yes",        type=int, default=10, help="How many yes-profiles to collect")
    p_scrape.add_argument("--batch-size", type=int, default=30, help="Follower-bios per batch")
    p_scrape.add_argument("--url",        default="https://scrape-orchestrator-mhnsdh4esa-ew.a.run.app/run-scrape",
                          help="Full URL of your Google Cloud Run /scrape endpoint")

    args = parser.parse_args()

    if args.cmd == "login":
        asyncio.run(interactive_login(Path(args.state)))
    else:  # scrape
        results = asyncio.run(
            run_remote_scrape(
                state_path = Path(args.state),
                target     = args.target,
                api_url    = args.url,
                target_yes = args.yes,
                batch_size = args.batch_size,
            )
        )
        # pretty-print
        print(json.dumps(results, indent=2))