# commands.md

## create venv

python3 -m venv venv

## activate venv

source venv/bin/activate

## install requirements

pip install requests python-dotenv

## freeze requirements

pip freeze > requirements.txt

# EXTRACT

## fetch OAuth token (run first)

python extract/api_calls/fetch_token.py

## fetch dam resources (last year)

python extract/api_calls/fetch_dam_resources.py

## fetch latest dam resources

python extract/api_calls/fetch_dam_resources_latest.py


# TRANSFORM

python transform/transform_latest_data.py

python transform/transform_dam_resources.py


# LOAD