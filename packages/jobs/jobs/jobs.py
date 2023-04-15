from dagster import ScheduleDefinition, define_asset_job, AssetSelection

from jobs.assets import DBT_KEY

dbt_job = define_asset_job(
    "dbt_job",
    selection=AssetSelection.groups(DBT_KEY),
)

dbt_job_schedule = ScheduleDefinition(job=dbt_job, cron_schedule="@hourly")

ALL_JOBS = [dbt_job]
ALL_SCHEDULES = [dbt_job_schedule]
