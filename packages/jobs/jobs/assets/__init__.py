from dagster import file_relative_path, load_assets_from_package_module
from dagster_dbt import load_assets_from_dbt_project
from jobs.assets import core
from jobs.resources import DBT_PROFILES, DBT_PROJECT_PATH

CORE = "core"

core_assets = load_assets_from_package_module(package_module=core, group_name=CORE)

dbt_assets = load_assets_from_dbt_project(
    profiles_dir=DBT_PROFILES, project_dir=DBT_PROJECT_PATH, key_prefix=["dbt"]
)
