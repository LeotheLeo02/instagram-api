# app/scraper.py
"""
Helper utilities for triggering the remote scraper.

Only :func:`run_remote_scrape` is exported.  The login step is now
handled by the desktop application, so the previous ``interactive_login``
helper has been removed.
"""

import asyncio
import json
from pathlib import Path
from typing import Dict

import httpx


################################################################################
# 1.  Invoke the remote scraper
################################################################################

async def run_remote_scrape(
    *,
    state_gcs_uri: str,
    target: str,
    api_url: str = "https://scrape-orchestrator-mhnsdh4esa-ew.a.run.app/run-scrape",
    target_yes: int = 12,
    batch_size: int = 30,
    num_bio_pages: int = 3
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
        "state_gcs_uri": state_gcs_uri,
        "num_bio_pages": num_bio_pages
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
# 2.  Convenience CLI:  ``python -m app.scraper``
################################################################################

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Trigger the remote scrape job")
    parser.add_argument("target", help="Instagram account to analyse (followers are scanned)")
    parser.add_argument("--state", required=True, help="gs:// path to saved Playwright cookies")
    parser.add_argument("--yes", type=int, default=10, help="How many yes-profiles to collect")
    parser.add_argument("--batch-size", type=int, default=30, help="Follower-bios per batch")
    parser.add_argument("--num-bio-pages", type=int, default=3, help="Number of bio pages to use")
    parser.add_argument(
        "--url",
        default="https://scrape-orchestrator-mhnsdh4esa-ew.a.run.app/run-scrape",
        help="Full URL of your Google Cloud Run /scrape endpoint",
    )

    args = parser.parse_args()

    results = asyncio.run(
        run_remote_scrape(
            state_gcs_uri=args.state,
            target=args.target,
            api_url=args.url,
            target_yes=args.yes,
            batch_size=args.batch_size,
            num_bio_pages=args.num_bio_pages
        )
    )

    # pretty-print
    print(json.dumps(results, indent=2))
