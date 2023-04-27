from dagster import (
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    job,
    define_op_job,
)

from dags.assets import DBT_ASSETS, DBT_KEY

ALL_JOBS = []
ALL_SCHEDULES = []

# DBT
dbt_job = define_asset_job("dbt_job", selection=AssetSelection.assets(*DBT_ASSETS))
dbt_job_schedule = ScheduleDefinition(job=dbt_job, cron_schedule="@hourly")
ALL_JOBS.append(dbt_job)
ALL_SCHEDULES.append(dbt_job_schedule)


# Current Price
current_price_job = define_asset_job(
    "current_price_job",
    selection=AssetSelection.groups("current_price"),
)
current_price_job_schedule = ScheduleDefinition(
    job=current_price_job, cron_schedule="* * * * *"
)

ALL_JOBS.append(current_price_job)
ALL_SCHEDULES.append(current_price_job_schedule)
