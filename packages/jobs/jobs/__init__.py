from dagster import (
    Definitions,
)

from jobs.assets import ALL_ASSETS
from jobs.resources import RESOURCES
from jobs.jobs import ALL_JOBS, ALL_SCHEDULES

defs = Definitions(
    assets=ALL_ASSETS, resources=RESOURCES, jobs=ALL_JOBS, schedules=ALL_SCHEDULES
)
