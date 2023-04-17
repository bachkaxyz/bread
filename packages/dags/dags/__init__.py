from dagster import (
    Definitions,
)

from dags.assets import ALL_ASSETS
from dags.resources import RESOURCES
from dags.jobs import ALL_JOBS, ALL_SCHEDULES

defs = Definitions(
    assets=ALL_ASSETS, resources=RESOURCES, jobs=ALL_JOBS, schedules=ALL_SCHEDULES
)
