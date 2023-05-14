from dagster import (
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    job,
)

from dags.assets import DBT_ASSETS, DBT_KEY

ALL_JOBS = []
ALL_SCHEDULES = []

# IBC
ibc_job = define_asset_job("ibc_job", selection=AssetSelection.groups("ibc"))
ibc_job_schedule = ScheduleDefinition(job=ibc_job, cron_schedule="@hourly")
ALL_JOBS.append(ibc_job)
ALL_SCHEDULES.append(ibc_job_schedule)

# TX's
txs_job = define_asset_job("txs_job", selection=AssetSelection.groups("txs"))
txs_job_schedule = ScheduleDefinition(job=txs_job, cron_schedule="@hourly")
ALL_JOBS.append(txs_job)
ALL_SCHEDULES.append(txs_job_schedule)

# Gas
gas_job = define_asset_job("gas_job", selection=AssetSelection.groups("gas"))
gas_job_schedule = ScheduleDefinition(job=gas_job, cron_schedule="@hourly")
ALL_JOBS.append(gas_job)
ALL_SCHEDULES.append(gas_job_schedule)

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

# Jackal Providers

current_providers_job = define_asset_job(
    "current_providers_job",
    selection=AssetSelection.groups("jackal_providers"),
)
current_providers_job_schedule = ScheduleDefinition(
    job=current_providers_job, cron_schedule="@hourly"
)

# Jackal Storage
storage_job = define_asset_job(
    "storage_job", selection=AssetSelection.groups("storage")
)
storage_job_schedule = ScheduleDefinition(job=storage_job, cron_schedule="@hourly")
ALL_JOBS.append(storage_job)
ALL_SCHEDULES.append(storage_job_schedule)


ALL_JOBS.append(current_providers_job)
ALL_SCHEDULES.append(current_providers_job_schedule)
