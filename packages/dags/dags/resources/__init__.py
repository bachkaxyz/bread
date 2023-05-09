import os
from asyncpg import create_pool
from dagster_dbt import dbt_cli_resource
from dagster import file_relative_path

# from dagster_gcp.gcs import gcs_resource, gcs_pickle_io_manager, gcs_file_manager
from dags.resources.postgres_resource import PostgresResource
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

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "")
POSTGRES_USER = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = quote_plus(os.getenv("POSTGRES_PASSWORD", ""))
POSTGRES_DB = os.getenv("POSTGRES_DB", "")
JOB_SCHEMA = os.getenv("JOB_SCHEMA", "public")

postgres_resource = PostgresResource(
    _pool=create_pool(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
    ),
    _schema=JOB_SCHEMA,
)


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


ALL_RESOURCES = {
    "dbt": dbt_resource,
    "postgres": postgres_resource,
    # "gcs": gcs,
    # "gcs_fm": gcs_fm,
    # "gcs_io_manager": gcs_io_manager,
}
