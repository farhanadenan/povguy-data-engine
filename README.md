# povguy-data-engine

Weekly Singapore property data fetcher. Pulls fresh data from URA, data.gov.sg, and Farhan's Apps Script endpoints, normalises into JSON snapshots, publishes as a GitHub Release.

`povguy-content-engine` consumes these snapshots each morning to build the daily carousel.

## Sources

| Source | Frequency | Coverage |
|---|---|---|
| URA Private Property Transactions API | Weekly | Condo + landed sales (4 batches = 1 year) |
| URA Private Property Rentals API | Weekly | Condo rentals (4 batches) |
| data.gov.sg HDB Resale | Weekly | Resale prices, by town/flat-type |
| data.gov.sg HDB BTO Subscription | Monthly | BTO oversubscription rates |
| Apps Script: condo-analysis | Daily | Curated condo signals |
| Apps Script: launches-pipeline | Daily | New launch tracker |
| Apps Script: rental-yield (v3) | Daily | Net yield calculations |
| Apps Script: distress-radar | Weekly | Undervalued listings |

## Run

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill keys
python main.py --all                # full weekly refresh
python main.py --source ura         # one source only
python main.py --source datagov.hdb-resale
```

Output: `snapshots/YYYY-MM-DD/{source}.json`

## Schedule

GitHub Actions cron: Sunday 22:00 SGT (Sunday 14:00 UTC).
On success, creates a GitHub Release tagged `snapshot-YYYY-MM-DD` with all JSON files attached.
