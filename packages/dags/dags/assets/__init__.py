from dagster import (
    load_assets_from_package_module,
)
from dagster_dbt import load_assets_from_dbt_project
from dags.assets import core
from dags.resources import DBT_PROFILES, DBT_PROJECT_PATH

CORE_KEY = "core"
DBT_KEY = "dbt"

CORE_ASSETS = load_assets_from_package_module(
    package_module=core, key_prefix=[CORE_KEY]
)

DBT_ASSETS = load_assets_from_dbt_project(
    profiles_dir=DBT_PROFILES,
    project_dir=DBT_PROJECT_PATH,
    key_prefix=[DBT_KEY],
)
ALL_ASSETS = [*CORE_ASSETS, *DBT_ASSETS]
