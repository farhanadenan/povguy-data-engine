#!/usr/bin/env python3
"""
Weekly data refresh orchestrator.
Pulls all sources, writes JSON snapshots to snapshots/YYYY-MM-DD/.
"""
import os
import sys
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from scrapers.ura import URAClient
from scrapers.datagov import DataGovClient
from scrapers.distress import DistressRadarClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

load_dotenv()

SNAPSHOTS_DIR = Path(os.getenv("SNAPSHOTS_DIR", "./snapshots"))


def write_snapshot(date_str: str, name: str, data) -> Path:
    out_dir = SNAPSHOTS_DIR / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{name}.json"
    out_file.write_text(json.dumps(data, indent=2, default=str))
    log.info(f"Wrote {out_file} ({out_file.stat().st_size:,} bytes)")
    return out_file


def fetch_ura(date_str: str):
    key = os.getenv("URA_ACCESS_KEY")
    if not key:
        log.warning("URA_ACCESS_KEY missing — skipping URA")
        return
    client = URAClient(access_key=key)

    # Resale + sub-sale transactions (4 batches, ~36 months)
    txns = client.fetch_residential_transactions()
    split = client.split_condo_landed(txns)
    write_snapshot(date_str, "ura-condo-transactions", split["condo"])
    write_snapshot(date_str, "ura-landed-transactions", split["landed"])

    # Rentals — last 6 quarters via refPeriod=YYqQ
    rentals = client.fetch_residential_rentals(quarters=6)
    write_snapshot(date_str, "ura-rentals", rentals)

    # New-launch universe + monthly developer-sales digest
    pipeline = client.fetch_pipeline()
    write_snapshot(date_str, "ura-pipeline", pipeline)

    dev_sales, dev_periods = client.fetch_developer_sales(months=6)
    write_snapshot(date_str, "ura-developer-sales", {
        "refPeriods": dev_periods,
        "records": dev_sales,
    })


def fetch_datagov(date_str: str):
    client = DataGovClient(api_key=os.getenv("DATAGOV_API_KEY", ""))
    # bto-launch intentionally excluded — no stable data.gov.sg resource ID;
    # URA pipeline + developer-sales feeds cover new launches.
    for key in ("hdb-resale", "hdb-rental"):
        try:
            data = client.fetch_dataset(key)
            write_snapshot(date_str, f"datagov-{key}", data)
        except Exception as e:
            log.warning(f"datagov {key}: {e}")


def fetch_appscript(date_str: str):
    """Apps Script feeds + the public Distress Radar HTML on povguy.sg.

    The Apps Script web-app for distress-radar is a heartbeat-only stub —
    the actual data lives in static HTML snapshots that the
    povguy-distress-radar skill publishes weekly to povguy.sg/dash/.
    We fetch the latest of those and write it as appscript-distress-radar.json
    so content-engine doesn't have to know the difference.

    The other 3 Apps Script feeds (new-launch, condo-analysis, rental-yield)
    duplicate URA — we don't fetch them.
    """
    distress_client = DistressRadarClient()
    snap_date, sections = distress_client.fetch_latest()
    if sections:
        payload = {
            "source": "povguy.sg/dash",
            "snapshot_date": snap_date.isoformat(),
            "fetched_at": datetime.now().isoformat(),
            "sections": sections,
            "total_listings": sum(s.get("count", 0) for s in sections),
        }
        write_snapshot(date_str, "appscript-distress-radar", payload)
    else:
        log.warning("Distress Radar: no snapshot found in lookback window")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Fetch all sources")
    parser.add_argument("--source", help="Specific source: ura | datagov | appscript")
    parser.add_argument("--date", help="Override snapshot date (YYYY-MM-DD)")
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    log.info(f"Snapshot date: {date_str}")

    sources = []
    if args.all or not args.source or args.source == "all":
        sources = ["ura", "datagov", "appscript"]
    else:
        sources = [args.source]
    log.info(f"Sources to fetch: {sources}")

    if "ura" in sources:
        fetch_ura(date_str)
    if "datagov" in sources:
        fetch_datagov(date_str)
    if "appscript" in sources:
        fetch_appscript(date_str)

    log.info("Done.")


if __name__ == "__main__":
    sys.exit(main() or 0)
