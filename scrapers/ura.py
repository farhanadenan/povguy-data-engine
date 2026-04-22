"""
URA scraper with proper token-exchange flow.

Endpoints used (mirrors the patterns proven in our Google Apps Script feeds):
  PMI_Resi_Transaction       — batch=1..4, ~36 months of resale/sub-sale txns
  PMI_Resi_Rental            — refPeriod=YYqQ (e.g. '26q1'), per-quarter rentals
  PMI_Resi_Pipeline          — no params, upcoming + ongoing project pipeline
  PMI_Resi_Developer_Sales   — refPeriod=MMYY, monthly developer sales digest

URA tokens: AccessKey -> daily Token via insertNewToken endpoint, ~24h TTL.
"""
import os
import time
import logging
from datetime import date
from typing import Dict, List, Optional, Tuple
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

TOKEN_URL = "https://eservice.ura.gov.sg/uraDataService/insertNewToken/v1"
DATA_URL = "https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1"

UA = "Mozilla/5.0 (compatible; PovGuyDataEngine/0.1)"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _recent_quarters(n: int, today: Optional[date] = None) -> List[str]:
    """Return last `n` completed quarters in URA's 'YYqQ' format (newest first).

    URA rental data lags ~1 quarter, so we always start at the LAST COMPLETED
    quarter, not the in-progress one. Mirrors `_recentQuarters` in
    rental-yield-apps-script.gs.
    """
    today = today or date.today()
    year = today.year
    month = today.month
    q = ((month - 1) // 3) + 1   # current quarter 1..4
    q -= 1                        # step back to last completed
    if q < 1:
        q = 4
        year -= 1
    out = []
    for _ in range(n):
        out.append(f"{str(year)[-2:]}q{q}")
        q -= 1
        if q < 1:
            q = 4
            year -= 1
    return out


def _recent_months_mmyy(n: int, today: Optional[date] = None) -> List[str]:
    """Return last `n` months in URA's 'MMYY' format (newest first).

    Dev-sales digest is published on the 15th for the prior month, so before
    the 15th we step back two months. Mirrors `latestOffset` logic in
    launches-pipeline-apps-script.gs.
    """
    today = today or date.today()
    offset = 1 if today.day >= 15 else 2
    y, m = today.year, today.month
    # step back by initial offset
    for _ in range(offset):
        m -= 1
        if m < 1:
            m = 12
            y -= 1
    out = []
    for _ in range(n):
        out.append(f"{m:02d}{str(y)[-2:]}")
        m -= 1
        if m < 1:
            m = 12
            y -= 1
    return out


def _range_mid(s: Optional[str]) -> Optional[float]:
    """URA returns sizes/rents as 'lo-hi' strings (e.g. '1500-2000'). Return midpoint."""
    if not s or not isinstance(s, str):
        return None
    if "-" in s:
        parts = s.split("-", 1)
        try:
            lo = float(parts[0].strip())
            hi = float(parts[1].strip())
            return (lo + hi) / 2.0
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# client
# ---------------------------------------------------------------------------

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
    def _fetch(self, service: str, params: Optional[Dict[str, str]] = None) -> Dict:
        """Generic GET. Pass any combo of {batch, refPeriod, ...}."""
        q = {"service": service}
        if params:
            q.update(params)
        res = requests.get(
            DATA_URL,
            params=q,
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

    # -------------------------- residential transactions ------------------

    def fetch_residential_transactions(self) -> List[Dict]:
        """PMI_Resi_Transaction — 4 batches, ~36 months of resale + sub-sale txns."""
        all_txns = []
        for batch in range(1, 5):
            data = self._fetch("PMI_Resi_Transaction", {"batch": str(batch)})
            for project in data.get("Result", []):
                proj_meta = {
                    "project": project.get("project"),
                    "street": project.get("street"),
                    "marketSegment": project.get("marketSegment"),
                    "x": project.get("x"),
                    "y": project.get("y"),
                }
                for txn in project.get("transaction", []):
                    all_txns.append({**txn, **proj_meta})
            time.sleep(1)
        logger.info(f"URA residential transactions: {len(all_txns)} records")
        return all_txns

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

    # -------------------------- rentals -----------------------------------

    def fetch_residential_rentals(self, quarters: int = 6) -> List[Dict]:
        """PMI_Resi_Rental — last `quarters` completed quarters via refPeriod=YYqQ.

        URA rental Result is NESTED: Result[].rental[] (not Result[].transaction[]).
        Each rental record carries area as range strings + leaseDate 'MMYY'.
        We flatten + add midpoint helpers + parse leaseDate to ISO month.
        """
        all_rentals = []
        for refp in _recent_quarters(quarters):
            try:
                data = self._fetch("PMI_Resi_Rental", {"refPeriod": refp})
            except Exception as e:
                logger.warning(f"URA rentals fetch failed for {refp}: {e}")
                continue

            if data.get("Status") != "Success":
                logger.warning(f"URA rentals {refp}: {data.get('Message') or data}")
                continue

            for project in data.get("Result", []):
                proj_meta = {
                    "project": project.get("project"),
                    "street": project.get("street"),
                }
                for r in project.get("rental", []):
                    lease_mmyy = r.get("leaseDate")  # 'MMYY' e.g. '0126'
                    iso_month = None
                    if lease_mmyy and len(lease_mmyy) == 4:
                        try:
                            mm = int(lease_mmyy[:2])
                            yy = int(lease_mmyy[2:])
                            iso_month = f"20{yy:02d}-{mm:02d}"
                        except ValueError:
                            pass
                    all_rentals.append({
                        **proj_meta,
                        "refPeriod": refp,
                        "propertyType": r.get("propertyType"),
                        "district": r.get("district"),
                        "noOfBedRoom": r.get("noOfBedRoom"),
                        "areaSqm": r.get("areaSqm"),
                        "areaSqft": r.get("areaSqft"),
                        "areaSqmMid": _range_mid(r.get("areaSqm")),
                        "areaSqftMid": _range_mid(r.get("areaSqft")),
                        "leaseDate": lease_mmyy,
                        "leaseMonthIso": iso_month,
                        "rent": r.get("rent"),
                    })
            time.sleep(1)
        logger.info(f"URA residential rentals: {len(all_rentals)} records across {quarters} quarters")
        return all_rentals

    # -------------------------- pipeline + dev sales ----------------------

    def fetch_pipeline(self) -> List[Dict]:
        """PMI_Resi_Pipeline — full upcoming + ongoing project pipeline.

        Used to populate the universe of new-launch projects. Joined later
        with developer-sales digest by composite (name|street) key.
        """
        try:
            data = self._fetch("PMI_Resi_Pipeline")
        except Exception as e:
            logger.warning(f"URA pipeline fetch failed: {e}")
            return []
        if data.get("Status") != "Success":
            logger.warning(f"URA pipeline: {data.get('Message') or data}")
            return []
        out = list(data.get("Result", []))
        logger.info(f"URA pipeline projects: {len(out)}")
        return out

    def fetch_developer_sales(self, months: int = 6) -> Tuple[List[Dict], List[str]]:
        """PMI_Resi_Developer_Sales — last `months` of monthly developer sales digests.

        Returns (records, refPeriods_fetched). Each record has refPeriod tagged
        so downstream can compute monthlySales arrays + last-3-month absorption.
        """
        all_records = []
        used_periods = []
        for refp in _recent_months_mmyy(months):
            try:
                data = self._fetch("PMI_Resi_Developer_Sales", {"refPeriod": refp})
            except Exception as e:
                logger.warning(f"URA dev-sales fetch failed for {refp}: {e}")
                continue
            if data.get("Status") != "Success":
                logger.warning(f"URA dev-sales {refp}: {data.get('Message') or data}")
                continue
            for project in data.get("Result", []):
                # The dev-sales response nests per-project monthly slices under .developerSales[]
                # but some payload variants put fields directly on the project. Handle both.
                if isinstance(project.get("developerSales"), list):
                    for ds in project.get("developerSales", []):
                        all_records.append({
                            "project": project.get("project"),
                            "street": project.get("street"),
                            "refPeriod": refp,
                            **ds,
                        })
                else:
                    all_records.append({**project, "refPeriod": refp})
            used_periods.append(refp)
            time.sleep(1)
        logger.info(
            f"URA developer sales: {len(all_records)} records across {len(used_periods)} months"
        )
        return all_records, used_periods
