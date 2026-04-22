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
from scrapers.appscript import AppScriptClient

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
    endpoints = {
        "new-launch": os.getenv("APPSCRIPT_NEW_LAUNCH"),
        "condo-analysis": os.getenv("APPSCRIPT_CONDO_ANALYSIS"),
        "rental-yield": os.getenv("APPSCRIPT_RENTAL_YIELD"),
        "distress-radar": os.getenv("APPSCRIPT_DISTRESS_RADAR"),
    }
    endpoints = {k: v for k, v in endpoints.items() if v}
    if not endpoints:
        log.warning("No Apps Script endpoints configured")
        return
    client = AppScriptClient(endpoints)
    data = client.fetch_all()
    for name, payload in data.items():
        write_snapshot(date_str, f"appscript-{name}", payload)


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
