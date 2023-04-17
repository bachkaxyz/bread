from dagster import (
    Definitions,
)

from dag.assets import ALL_ASSETS
from dag.resources import RESOURCES
from dag.jobs import ALL_JOBS, ALL_SCHEDULES

defs = Definitions(
    assets=ALL_ASSETS, resources=RESOURCES, jobs=ALL_JOBS, schedules=ALL_SCHEDULES
)
