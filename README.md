# Water Dashboard NSW ETL

## Project Overview

ETL pipeline for extracting NSW dam water data from the [WaterInsights API](https://api.nsw.gov.au/Product/Index/26), transforming it for analysis, and loading it into a MySQL database.

## Quick Start

```bash
# Setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# Run full pipeline
python scripts/run_etl_pipeline.py
```

## Configuration

Create a `.env` file with:

```
API_KEY=your_waterinsights_api_key
API_SECRET=your_waterinsights_api_secret
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=your_db_name
```

## Pipeline Stages

| Stage | Description | Output |
|-------|-------------|--------|
| **Extract** | Fetches dam data from WaterInsights API | `data/input_data/` |
| **Transform** | Flattens and normalizes JSON data | `data/output_data/` |
| **Load** | Inserts data into MySQL database | `latest_data`, `dam_resources` tables |

### Running Individual Stages

```bash
# Extract
python extract/api_calls/fetch_token.py
python extract/api_calls/fetch_dam_resources_latest.py
python extract/api_calls/fetch_dam_resources.py

# Transform
python transform/transform_dam_resources_latest.py
python transform/transform_dam_resources.py

# Load
python load/load_latest_data.py
python load/load_dam_resources.py
```

## Testing

```bash
pytest tests/ -v
pytest tests/test_extract.py
pytest tests/test_transform.py
pytest tests/test_load.py
```

## Docker

### Local Development (with MySQL)

```bash
# Setup
cp .env.docker .env

# Build and start
./docker-run.sh build
./docker-run.sh up

# Run pipeline
./docker-run.sh pipeline
./docker-run.sh pipeline-no-tests

# Run individual stages
./docker-run.sh extract
./docker-run.sh transform
./docker-run.sh load

# Other commands
./docker-run.sh test
./docker-run.sh shell
./docker-run.sh db
./docker-run.sh down
```

### Manual Docker Commands

```bash
# Build
docker build -t water-etl .

# Run with external database
docker run --rm --env-file .env water-etl

# Run specific stage
docker run --rm --env-file .env water-etl --stage extract
docker run --rm --env-file .env water-etl --no-tests
```

## Known Issues

These dams are excluded due to API errors:
- `BlueMountainsTotal` - Returns API errors
- `401027` (Hume Dam) - Returns HTTP 204
