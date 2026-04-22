"""
Apps Script proxy.
Calls Farhan's existing Google Apps Script web apps, captures the JSON they return.

Each endpoint, when called with no params, returns a health-check.
The actual data surface uses query params — exact patterns vary per script.
For now we hit the bare URL and trust each script returns its current dataset.
"""
import logging
import requests
from typing import Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class AppScriptClient:
    def __init__(self, endpoints: Dict[str, str]):
        self.endpoints = endpoints

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
                logger.info(f"Fetching Apps Script: {name}")
                out[name] = self.fetch(name, params={"action": "fetch"})
            except Exception as e:
                logger.warning(f"Apps Script {name} failed: {e}")
                out[name] = {"error": str(e)}
        return out
