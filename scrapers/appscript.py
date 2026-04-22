"""
Apps Script proxy.

Each Apps Script web-app has its own `action` query param contract; calling
the bare URL returns a health-check JSON. We only fetch endpoints whose
data we can't get more cheaply from URA / data.gov.sg directly.

Currently in scope:
- distress-radar  (unique: scrapes PropertyGuru + CommercialGuru) → action=fetch

Out of scope (URA covers them natively):
- new-launch       → URA PMI_Resi_Pipeline + Developer_Sales
- condo-analysis   → URA PMI_Resi_Transaction (split condo/landed)
- rental-yield     → URA PMI_Resi_Rental + computed against transactions
"""
import logging
import requests
from typing import Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

# Per-endpoint default query params. Add new entries as we onboard more
# Apps Script feeds. Anything not in this map is skipped.
ENDPOINT_PARAMS = {
    "distress-radar": {"action": "fetch"},
}


class AppScriptClient:
    def __init__(self, endpoints: Dict[str, str]):
        # Filter to only endpoints we know how to call
        self.endpoints = {k: v for k, v in endpoints.items() if k in ENDPOINT_PARAMS}
        skipped = set(endpoints) - set(self.endpoints)
        if skipped:
            logger.info(f"Skipping Apps Script endpoints (covered by URA): {sorted(skipped)}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def fetch(self, name: str, params: Optional[Dict] = None) -> Dict:
        url = self.endpoints.get(name)
        if not url:
            raise KeyError(f"No Apps Script endpoint for {name}")
        # Apps Script web apps follow redirects to googleusercontent.com
        res = requests.get(url, params=params or {}, timeout=120, allow_redirects=True)
        res.raise_for_status()
        try:
            return res.json()
        except ValueError:
            return {"raw": res.text[:5000]}

    def fetch_all(self) -> Dict[str, Dict]:
        out = {}
        for name in self.endpoints:
            try:
                params = ENDPOINT_PARAMS.get(name, {})
                logger.info(f"Fetching Apps Script: {name} (params={params})")
                out[name] = self.fetch(name, params=params)
            except Exception as e:
                logger.warning(f"Apps Script {name} failed: {e}")
                out[name] = {"error": str(e)}
        return out
