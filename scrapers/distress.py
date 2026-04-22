"""
Distress Radar scraper.

Farhan publishes weekly Distress Radar snapshots to povguy.sg under
`dash/distress-YYYY-MM-DD.html`. Each snapshot embeds the full
listings dataset as a JS array literal:

    const SECTIONS = [
      { key:'hdb',   label:'HDB Resale',   ..., data:[{...}, ...] },
      { key:'condo', label:'Condo Resale', ..., data:[{...}, ...] },
      { key:'comm',  label:'Commercial',   ..., data:[{...}, ...] },
    ];

We walk back up to N days from the target snapshot date, fetch the most
recent available file, and extract the SECTIONS data into a structured
JSON payload.

Why this lives in data-engine and not appscript.py:
- The Apps Script endpoint (APPSCRIPT_DISTRESS_RADAR) is heartbeat-only.
- The actual data is published as static HTML to povguy.sg by the
  povguy-distress-radar skill that Farhan runs locally each week.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

PUBLIC_BASE = "https://povguy.sg/dash"
LOOKBACK_DAYS = 21

# Captures key, label, eyebrow, color, and the JSON-array data block in a single
# entry of the SECTIONS literal.
_SECTION_RE = re.compile(
    r"\{\s*key:\s*'(?P<key>[^']+)'\s*,"
    r"\s*label:\s*'(?P<label>[^']+)'\s*,"
    r"\s*eyebrow:\s*'(?P<eyebrow>[^']*)'\s*,"
    r"\s*color:\s*'(?P<color>[^']*)'\s*,"
    r"\s*data:\s*(?P<data>\[.*?\])\s*\}",
    re.DOTALL,
)


class DistressRadarClient:
    def __init__(self, base_url: str = PUBLIC_BASE, lookback_days: int = LOOKBACK_DAYS):
        self.base_url = base_url.rstrip("/")
        self.lookback_days = lookback_days
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "povguy-data-engine/1.0"})

    def _candidate_dates(self, anchor: date) -> List[date]:
        return [anchor - timedelta(days=i) for i in range(self.lookback_days + 1)]

    def _fetch(self, snapshot_date: date) -> Optional[str]:
        url = f"{self.base_url}/distress-{snapshot_date.isoformat()}.html"
        try:
            res = self.session.get(url, timeout=30)
        except requests.RequestException as e:
            logger.debug(f"  distress {snapshot_date}: request error {e}")
            return None
        if res.status_code == 404:
            return None
        if res.status_code != 200:
            logger.warning(f"  distress {snapshot_date}: HTTP {res.status_code}")
            return None
        return res.text

    def _parse_sections(self, html: str) -> List[Dict]:
        sections = []
        for m in _SECTION_RE.finditer(html):
            data_blob = m.group("data")
            try:
                listings = json.loads(data_blob)
            except json.JSONDecodeError as e:
                logger.warning(f"  distress section '{m.group('key')}' failed to parse: {e}")
                continue
            sections.append({
                "key": m.group("key"),
                "label": m.group("label"),
                "eyebrow": m.group("eyebrow"),
                "color": m.group("color"),
                "count": len(listings),
                "listings": listings,
            })
        return sections

    def fetch_latest(self, anchor: Optional[date] = None) -> Tuple[Optional[date], List[Dict]]:
        """Walks back from `anchor` (or today) up to lookback_days. Returns
        (snapshot_date, sections) for the most recent snapshot found.
        Returns (None, []) if nothing is available in the window."""
        anchor = anchor or date.today()
        for candidate in self._candidate_dates(anchor):
            html = self._fetch(candidate)
            if html is None:
                continue
            logger.info(f"  distress: found snapshot {candidate}")
            sections = self._parse_sections(html)
            if not sections:
                logger.warning(f"  distress {candidate}: HTML fetched but no SECTIONS parsed")
                continue
            return candidate, sections
        return None, []
