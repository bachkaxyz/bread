import asyncio
import os
from asyncpg import create_pool
from dagster_dbt import dbt_cli_resource
from dagster import file_relative_path

# from dagster_gcp.gcs import gcs_resource, gcs_pickle_io_manager, gcs_file_manager
from dags.resources.postgres_resource import PostgresResource
from dotenv import load_dotenv


load_dotenv()


ALL_RESOURCES = {}

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
ALL_RESOURCES["dbt"] = dbt_resource

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "")
POSTGRES_USER = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB = os.getenv("POSTGRES_DB", "")

JOB_SCHEMA = os.getenv("JOB_SCHEMA", "public")

postgres_resource = PostgresResource(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    database=POSTGRES_DB,
    s=JOB_SCHEMA,
)

ALL_RESOURCES["postgres"] = postgres_resource


async def init_postgres():
    await postgres_resource.create_schema()


loop = asyncio.get_event_loop()
loop.run_until_complete(init_postgres())

# "gcs": gcs,
# "gcs_fm": gcs_fm,
# "gcs_io_manager": gcs_io_manager,


# gcs = gcs_resource.configured(
#     {
#         "project": os.environ["GCP_PROJECT_ID"],
#     }
# )

# gcs_io_manager = gcs_pickle_io_manager.configured(
#     {"gcs_bucket": os.environ["BUCKET_NAME"], "gcs_prefix": os.environ["CHAIN_ID"]}
# )

# gcs_fm = gcs_file_manager.configured(
#     {"gcs_bucket": os.environ["BUCKET_NAME"], "gcs_prefix": os.environ["CHAIN_ID"]}
# )
