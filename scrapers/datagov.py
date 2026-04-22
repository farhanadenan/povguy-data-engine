"""
data.gov.sg scraper — uses the public v1 datastore_search endpoint.

This matches how POV Guy Site/POVGUY V2/hdb-market.html consumes the data live:
    https://data.gov.sg/api/action/datastore_search?resource_id=...&limit=5000

The v1 endpoint requires NO authentication, has no rate limit issues for our
batch sizes, and is the same source that powers Singapore's open-data portal.

We previously tried the v2 API which requires an api key and uses a clunky
poll-download flow — both unnecessary for our use case.

Reference: https://guide.data.gov.sg/developer-guide/dataset-apis
"""
import logging
import requests
from typing import Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

V1_BASE = "https://data.gov.sg/api/action/datastore_search"

# Resource IDs (from the data.gov.sg dataset pages)
DATASETS = {
    # HDB Resale Flat Prices (Jan 2017 onwards) — confirmed in hdb-market.html
    "hdb-resale": "f1765b54-a209-4718-8d38-a39237f502b3",
    # TODO: confirm these resource IDs from the dataset pages on data.gov.sg
    # "hdb-rental": "<resource_id>",
    # "bto-launch": "<resource_id>",
}


class DataGovClient:
    def __init__(self, api_key: str = ""):
        # api_key kept for backwards compat with main.py — v1 doesn't use it
        self.session = requests.Session()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def fetch_dataset(self, dataset_key: str, limit: int = 5000, sort: str = "month desc") -> List[Dict]:
        """Fetch all records from a dataset, paginating if needed."""
        resource_id = DATASETS.get(dataset_key, dataset_key)
        if resource_id == dataset_key and dataset_key not in DATASETS:
            logger.warning(f"Unknown dataset key '{dataset_key}' — treating as raw resource_id")

        all_records: List[Dict] = []
        offset = 0
        while True:
            params = {
                "resource_id": resource_id,
                "limit": limit,
                "offset": offset,
            }
            if sort:
                params["sort"] = sort
            res = self.session.get(V1_BASE, params=params, timeout=60)
            res.raise_for_status()
            body = res.json()
            if not body.get("success"):
                raise RuntimeError(f"data.gov.sg returned success=false: {body}")
            records = body.get("result", {}).get("records", [])
            all_records.extend(records)
            total = body.get("result", {}).get("total", 0)
            logger.info(f"  {dataset_key}: fetched {len(all_records)}/{total}")
            if len(records) < limit or len(all_records) >= total:
                break
            offset += limit
        return all_records
