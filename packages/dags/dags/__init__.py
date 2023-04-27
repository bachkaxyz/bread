from dagster import (
    Definitions,
)

from dags.assets import ALL_ASSETS
from dags.resources import ALL_RESOURCES
from dags.jobs import ALL_JOBS, ALL_SCHEDULES

defs = Definitions(
    assets=ALL_ASSETS,
    resources=ALL_RESOURCES,
    jobs=ALL_JOBS,
    schedules=ALL_SCHEDULES,
)
