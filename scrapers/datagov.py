"""
data.gov.sg scraper — uses the public v1 datastore_search endpoint.

We pass the API key via the `x-api-key` header on every request. Without
the header, data.gov.sg rate-limits us at ~10k records/day with HTTP 429.
With the header we can paginate through full datasets (250k+ records).

Retry policy: we retry **per page**, not the whole pagination loop. The
previous design wrapped `fetch_dataset` in `@retry`, which meant any
transient HTTPError mid-pagination restarted us from offset 0 and never
made progress (we'd just hit the same wall three times).

Reference:
- https://guide.data.gov.sg/developer-guide/dataset-apis
- https://guide.data.gov.sg/developer-guide/api-overview/api-authentication
"""
import logging
import time
import requests
from typing import Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

V1_BASE = "https://data.gov.sg/api/action/datastore_search"

# Resource IDs (verified against data.gov.sg dataset pages, April 2026)
DATASETS = {
    # HDB Resale Flat Prices (Jan 2017 onwards) — confirmed in hdb-market.html
    "hdb-resale": "f1765b54-a209-4718-8d38-a39237f502b3",
    # HDB Rental Records — verified 2026-04-22 (~194k records)
    "hdb-rental": "d_c9f57187485a850908655db0e8cfe651",
    # bto-launch resource ID intentionally omitted — no stable dataset on
    # data.gov.sg yet. URA pipeline + developer-sales feeds cover new launches.
}


class DataGovClient:
    def __init__(self, api_key: str = ""):
        self.api_key = api_key
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({"x-api-key": api_key})
        else:
            logger.warning("DATAGOV_API_KEY not set — rate-limited at ~10k records")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(min=2, max=30),
        retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
        reraise=True,
    )
    def _fetch_page(self, resource_id: str, limit: int, offset: int, sort: str = "") -> Dict:
        """Fetch ONE page. Retries only this page on transient errors."""
        params = {"resource_id": resource_id, "limit": limit, "offset": offset}
        if sort:
            params["sort"] = sort
        res = self.session.get(V1_BASE, params=params, timeout=60)
        # Surface 429s as retryable
        if res.status_code == 429:
            res.raise_for_status()
        res.raise_for_status()
        body = res.json()
        if not body.get("success"):
            raise RuntimeError(f"data.gov.sg returned success=false: {body}")
        return body.get("result", {})

    def fetch_dataset(self, dataset_key: str, limit: int = 5000, sort: str = "") -> List[Dict]:
        """Fetch all records, paginating with `offset`. Page-level retries
        ensure a transient blip doesn't reset our progress."""
        resource_id = DATASETS.get(dataset_key, dataset_key)
        if resource_id == dataset_key and dataset_key not in DATASETS:
            logger.warning(f"Unknown dataset key '{dataset_key}' — treating as raw resource_id")

        all_records: List[Dict] = []
        offset = 0
        while True:
            try:
                result = self._fetch_page(resource_id, limit, offset, sort)
            except Exception as e:
                logger.error(f"  {dataset_key}: page at offset={offset} failed permanently: {e}")
                # Return what we have so far — partial data is better than nothing
                if all_records:
                    logger.warning(f"  {dataset_key}: returning {len(all_records):,} partial records")
                    return all_records
                raise
            records = result.get("records", [])
            all_records.extend(records)
            total = result.get("total", 0)
            logger.info(f"  {dataset_key}: fetched {len(all_records):,}/{total:,}")
            if len(records) < limit or len(all_records) >= total:
                break
            offset += limit
            time.sleep(0.5)  # be polite to data.gov.sg
        return all_records
