# etl-water-dashboard-nsw

ETL pipeline for NSW dam water data from the WaterInsights API.

## Known Issues

The following dams are excluded from `fetch_dam_resources.py` and `fetch_dam_resources_latest.py` due to API errors:

- **BlueMountainsTotal** (Blue Mountains Dams) - Returns API errors
- **401027** (Hume Dam) - Returns HTTP 204