"""
data.gov.sg v2 scraper.

The v2 API uses a poll-download flow for large datasets:
  1. POST /v2/public/api/datasets/{id}/initiate-download   → returns download_url (signed)
  2. GET that signed URL to get the JSON/CSV
For smaller datasets (and the catalog), simple GET works.

Reference: https://data.gov.sg/developer
"""
import os
import time
import logging
import requests
from typing import Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

BASE = "https://api-production.data.gov.sg/v2/public"

# Known dataset IDs for SG property
DATASETS = {
    "hdb-resale": "d_8b84c4ee58e3cfc0ece0d773c8ca6abc",  # Resale flat prices (2017+)
    "hdb-rental": "d_c9f57187485a850908655db0e8cfe651",  # Renting out of HDB flats
    "bto-launch": "d_7c7b0e2ec56693b09f7c19a83a1e9b54",  # Sale of BTO flats subscription
}


class DataGovClient:
    def __init__(self, api_key: str = ""):
        self.api_key = api_key
        self.session = requests.Session()
        if api_key:
            # data.gov.sg v2 accepts the key as a query parameter on poll-download
            # for some endpoints, or as x-api-key header. We try both.
            self.session.headers.update({"x-api-key": api_key})

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _initiate(self, dataset_id: str) -> str:
        url = f"{BASE}/api/datasets/{dataset_id}/initiate-download"
        res = self.session.post(url, timeout=30)
        res.raise_for_status()
        body = res.json()
        # Poll until ready
        while True:
            poll = self.session.get(f"{BASE}/api/datasets/{dataset_id}/poll-download", timeout=30)
            pdata = poll.json()
            if pdata.get("data", {}).get("status") == "READY":
                return pdata["data"]["url"]
            time.sleep(2)

    def fetch_dataset(self, dataset_key: str, limit: int = 10_000) -> List[Dict]:
        ds_id = DATASETS.get(dataset_key, dataset_key)
        try:
            download_url = self._initiate(ds_id)
            res = self.session.get(download_url, timeout=60)
            res.raise_for_status()
            # Could be CSV or JSON depending on dataset; assume JSON for now
            return res.json() if "json" in res.headers.get("content-type", "") else res.text
        except Exception as e:
            logger.warning(f"data.gov.sg fetch failed for {dataset_key}: {e}")
            # Fallback to old v1 resource_id-based API for HDB resale
            if dataset_key == "hdb-resale":
                return self._v1_fallback("d_8b84c4ee58e3cfc0ece0d773c8ca6abc", limit)
            return []

    def _v1_fallback(self, resource_id: str, limit: int = 10_000) -> List[Dict]:
        url = "https://data.gov.sg/api/action/datastore_search"
        res = requests.get(url, params={"resource_id": resource_id, "limit": limit}, timeout=60)
        res.raise_for_status()
        return res.json().get("result", {}).get("records", [])
