# NSW Water Dashboard ETL

ETL pipeline for extracting NSW dam water data from the [WaterInsights API](https://api.nsw.gov.au/Product/Index/26), transforming it for analysis, and loading it into a MySQL database.

## Quick Start

```bash
# Setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # Edit with your credentials

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
python extract/api_calls/fetch_token.py              # Get OAuth token (run first)
python extract/api_calls/fetch_dam_resources_latest.py
python extract/api_calls/fetch_dam_resources.py

# Transform
python transform/transform_dam_resources_latest.py
python transform/transform_dam_resources.py

# Load
python load/load_latest_data.py      # Replaces latest_data table
python load/load_dam_resources.py    # Appends to dam_resources table
```

## Testing

```bash
pytest tests/ -v              # All tests (42 total)
pytest tests/test_extract.py  # Extract tests
pytest tests/test_transform.py # Transform tests
pytest tests/test_load.py     # Load tests (requires DB)
```

## Project Structure

```
├── extract/api_calls/    # API data extraction scripts
├── transform/            # Data transformation scripts
├── load/                 # Database loading scripts
├── schemas/              # Pydantic validation models
├── tests/                # Pytest test suites
├── scripts/              # Utility and pipeline scripts
├── data/
│   ├── input_data/       # Raw extracted JSON
│   └── output_data/      # Transformed JSON
└── sql/                  # Database schema
```

## Docker

### Local Development (with MySQL)

```bash
# Setup
cp .env.docker .env  # Edit with your API credentials

# Build and start
./docker-run.sh build
./docker-run.sh up        # Starts MySQL container

# Run pipeline
./docker-run.sh pipeline  # Full pipeline with tests
./docker-run.sh pipeline-no-tests

# Run individual stages
./docker-run.sh extract
./docker-run.sh transform
./docker-run.sh load

# Other commands
./docker-run.sh test      # Run tests only
./docker-run.sh shell     # Open shell in container
./docker-run.sh db        # Connect to MySQL
./docker-run.sh down      # Stop containers
```

### Production (Hostinger VPS)

1. Copy files to VPS:
```bash
scp -r . user@your-vps:/path/to/app
```

2. Configure `.env` with your production database credentials

3. Run pipeline:
```bash
./docker-run.sh prod
# Or directly:
docker-compose -f docker-compose.prod.yml run --rm etl
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
