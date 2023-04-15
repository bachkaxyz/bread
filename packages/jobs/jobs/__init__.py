import os

from dagster import (
    Definitions,
    load_assets_from_modules,
    load_assets_from_package_module,
)
from dagster_dbt import dbt_cli_resource

from jobs.assets import core_assets, dbt_assets
from jobs.resources import DBT_PROFILES, DBT_PROJECT_PATH, RESOURCES

all_assets = [*core_assets, *dbt_assets]


defs = Definitions(assets=all_assets, resources=RESOURCES)
