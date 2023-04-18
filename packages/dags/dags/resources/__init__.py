import os
from dagster_dbt import dbt_cli_resource
from dagster import file_relative_path
from dags.resources.postgres_io_manager import DbIOManager
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

DBT_PROJECT_PATH = file_relative_path(__file__, "../../dbt_project")
DBT_PROFILES = DBT_PROJECT_PATH + "/config"

# if larger project use load_assets_from_dbt_manifest
# dbt_assets = load_assets_from_dbt_manifest(json.load(DBT_PROJECT_PATH + "manifest.json", encoding="utf8"))
dbt_resource = dbt_cli_resource.configured(
    {
        "project_dir": DBT_PROJECT_PATH,
        "profiles_dir": DBT_PROFILES,
        "target": "dev",
    }
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = quote_plus(os.getenv("POSTGRES_PASSWORD", ""))
POSTGRES_DB = os.getenv("POSTGRES_DB")

JOB_SCHEMA = os.getenv("JOB_SCHEMA", "public")

POSTGRES_CON_STRING = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

postgres_resource = DbIOManager(con_string=POSTGRES_CON_STRING, schema=JOB_SCHEMA)

RESOURCES = {"dbt": dbt_resource, "postgres": postgres_resource}
