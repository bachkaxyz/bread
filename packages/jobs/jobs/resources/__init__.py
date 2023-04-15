from dagster_dbt import dbt_cli_resource
from dagster import file_relative_path

DBT_PROJECT_PATH = file_relative_path(__file__, "../../dbt_project")
DBT_PROFILES = DBT_PROJECT_PATH + "/config"

# if larger project use load_assets_from_dbt_manifest
# dbt_assets = load_assets_from_dbt_manifest(json.load(DBT_PROJECT_PATH + "manifest.json", encoding="utf8"))
dbt_resource = dbt_cli_resource.configured(
    {
        "project_dir": DBT_PROJECT_PATH,
        "profiles_dir": DBT_PROFILES,
        "key_prefix": ["dbt"],
        "target": "dev",
    }
)

RESOURCES = {"dbt": dbt_resource}
