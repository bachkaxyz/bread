from dagster import ScheduleDefinition, define_asset_job, AssetSelection

from dag.assets import DBT_ASSETS, DBT_KEY

dbt_job = define_asset_job("dbt_job", selection=AssetSelection.assets(*DBT_ASSETS))

# current_price_job = define_asset_job(
#     "current_price_job",
#     selection= AssetSelection.keys("core/current_price/*"),
# )

dbt_job_schedule = ScheduleDefinition(job=dbt_job, cron_schedule="@hourly")
# current_price_job_schedule = ScheduleDefinition(
#     job=current_price_job, cron_schedule="* * * * *"
# )

ALL_JOBS = [dbt_job]  # ,current_price_job]
ALL_SCHEDULES = [dbt_job_schedule]  # , current_price_job_schedule]
