# commands.md

## create venv

python3 -m venv venv

## activate venv

source venv/bin/activate

## install requirements

pip install -r requirements.txt

## freeze requirements

pip freeze > requirements.txt



# DATABASE CONFIGURATION

## configure .env file

Copy .env.example to .env and configure your credentials:

cp .env.example .env
nano .env  # or vim/code .env

## switch database provider

# For local MySQL
DB_PROVIDER=local
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=water_dashboard

# For Supabase PostgreSQL
DB_PROVIDER=supabase
SUPABASE_DB_HOST=db.xxxxxxxxxxxxx.supabase.co
SUPABASE_DB_PORT=5432
SUPABASE_DB_USER=postgres.xxxxxxxxxxxxx
SUPABASE_DB_PASSWORD=your_password
SUPABASE_DB_NAME=postgres

## test database connection

python scripts/db_connect.py



# SCRIPTS

python scripts/db_connect.py

python scripts/db_seed_latest_data.py

python scripts/db_seed_dam_resources.py

python scripts/db_test_queries.py



# EXTRACT

## fetch OAuth token (run first)

python extract/api_calls/fetch_token.py

## fetch dam resources (last year)

python extract/api_calls/fetch_dam_resources.py

## fetch latest dam resources

python extract/api_calls/fetch_dam_resources_latest.py



# TRANSFORM

python transform/transform_dam_resources_latest.py

python transform/transform_dam_resources.py



# LOAD

python load/load_dam_resources.py

python load/load_latest_data.py



# PIPELINE

## run full ETL pipeline with tests

python scripts/run_etl_pipeline.py

## run full ETL pipeline without tests

python scripts/run_etl_pipeline.py --no-tests

## run specific stage only

python scripts/run_etl_pipeline.py --stage extract
python scripts/run_etl_pipeline.py --stage transform
python scripts/run_etl_pipeline.py --stage load



# TESTS

## run all tests

pytest tests/ -v

## run extract tests only

pytest tests/test_extract.py -v

## run transform tests only

pytest tests/test_transform.py -v

## run load tests only

pytest tests/test_load.py -v



# DOCKER

Note: Supports local MySQL or remote Supabase (set DB_PROVIDER in .env)
- DB_PROVIDER=local    → MySQL on host machine
- DB_PROVIDER=supabase → PostgreSQL (Supabase)

## build image

./docker-run.sh build

## run full pipeline with tests

./docker-run.sh pipeline

## run full pipeline without tests

./docker-run.sh pipeline-no-tests

## run specific stages

./docker-run.sh extract
./docker-run.sh transform
./docker-run.sh load

## run tests

./docker-run.sh test

## open shell in container

./docker-run.sh shell

## view logs

./docker-run.sh logs

## stop containers

./docker-run.sh down

## run with external DB (production)

./docker-run.sh prod