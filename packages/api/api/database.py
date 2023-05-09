import os
import databases
from dotenv import load_dotenv
from urllib.parse import quote_plus


load_dotenv()

host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
user = os.getenv("POSTGRES_USER")
password = quote_plus(os.getenv("POSTGRES_PASSWORD", ""))
database = os.getenv("POSTGRES_DB")

DBT_SCHEMA = f'"{os.getenv("DBT_SCHEMA", "public")}"'
INDEXER_SCHEMA = f'"{os.getenv("INDEXER_SCHEMA", "public")}"'
DAGSTER_SCHEMA = f'"{os.getenv("DAGSTER_SCHEMA", "public")}"'

database_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
if database_url is None:
    raise Exception("POSTGRES_URI environment variable not set")
database = databases.Database(database_url)
