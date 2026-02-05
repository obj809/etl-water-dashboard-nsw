# commands.md

## create venv

python3 -m venv venv

## activate venv

source venv/bin/activate

## install requirements

pip install requests python-dotenv

## freeze requirements

pip freeze > requirements.txt



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

Note: Requires local MySQL running on host machine

## build image

./docker-run.sh build

## run full pipeline

./docker-run.sh pipeline

## stop containers

./docker-run.sh down

## run with external DB (production)

./docker-run.sh prod