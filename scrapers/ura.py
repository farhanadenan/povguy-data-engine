"""
URA scraper with proper token-exchange flow.
URA requires AccessKey → daily Token via insertNewToken endpoint.
"""
import os
import time
import logging
from typing import Dict, List, Optional
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

TOKEN_URL = "https://eservice.ura.gov.sg/uraDataService/insertNewToken/v1"
DATA_URL = "https://www.ura.gov.sg/uraDataService/invokeUraDS"

UA = "Mozilla/5.0 (compatible; PovGuyDataEngine/0.1)"


class URAClient:
    def __init__(self, access_key: str):
        self.access_key = access_key
        self._token: Optional[str] = None
        self._token_fetched_at: float = 0

    def _get_token(self) -> str:
        # URA tokens last ~24h. Refresh every 12h to be safe.
        if self._token and (time.time() - self._token_fetched_at) < 43200:
            return self._token

        res = requests.get(
            TOKEN_URL,
            headers={"AccessKey": self.access_key, "User-Agent": UA},
            timeout=30,
        )
        res.raise_for_status()
        data = res.json()
        if data.get("Status") != "Success":
            raise RuntimeError(f"URA token exchange failed: {data}")
        self._token = data["Result"]
        self._token_fetched_at = time.time()
        logger.info("URA token refreshed")
        return self._token

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15))
    def _fetch(self, service: str, batch: int) -> Dict:
        res = requests.get(
            DATA_URL,
            params={"service": service, "batch": str(batch)},
            headers={
                "AccessKey": self.access_key,
                "Token": self._get_token(),
                "User-Agent": UA,
                "Accept": "application/json",
            },
            timeout=45,
        )
        res.raise_for_status()
        return res.json()

    def fetch_residential_transactions(self) -> List[Dict]:
        all_txns = []
        for batch in range(1, 5):
            data = self._fetch("PMI_Resi_Transaction", batch)
            for project in data.get("Result", []):
                for txn in project.get("transaction", []):
                    all_txns.append({**txn, "project": project.get("project"), "street": project.get("street")})
            time.sleep(1)
        logger.info(f"URA residential transactions: {len(all_txns)} records")
        return all_txns

    def fetch_residential_rentals(self) -> List[Dict]:
        all_rentals = []
        for batch in range(1, 5):
            data = self._fetch("PMI_Resi_Rental", batch)
            for project in data.get("Result", []):
                for r in project.get("rental", []):
                    all_rentals.append({**r, "project": project.get("project"), "street": project.get("street")})
            time.sleep(1)
        logger.info(f"URA residential rentals: {len(all_rentals)} records")
        return all_rentals

    def split_condo_landed(self, transactions: List[Dict]) -> Dict[str, List[Dict]]:
        condo, landed = [], []
        landed_keywords = ("DETACHED", "TERRACE", "BUNGALOW", "SEMI")
        for t in transactions:
            ptype = (t.get("propertyType") or "").upper()
            if any(k in ptype for k in landed_keywords):
                landed.append(t)
            else:
                condo.append(t)
        return {"condo": condo, "landed": landed}
