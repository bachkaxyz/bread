from api.database import database
from fastapi import APIRouter

from api.database import DBT_SCHEMA

router = APIRouter(
    prefix="/ibc", tags=["ibc"], responses={404: {"description": "Not found"}}
)


@router.get("/transfers/volume/hourly")
async def transfer_volume_hourly():
    return await database.fetch_all(
        f"""
        select *
        from {DBT_SCHEMA}.hourly_ibc_transfers
        """
    )


@router.get("/transfers/volume/daily")
async def transfer_volume_daily():
    return await database.fetch_all(
        f"""
        select *
        from {DBT_SCHEMA}.daily_ibc_transfers
        """
    )


@router.get("/transfers/cumulative/hourly")
async def transfer_cum_hourly():
    return await database.fetch_all(
        f"""
        select *
        from {DBT_SCHEMA}.hourly_cum_ibc_transfers
        """
    )


@router.get("/transfers/cumulative/daily")
async def transfer_cumulative_daily():
    return await database.fetch_all(
        f"""
        select *
        from {DBT_SCHEMA}.daily_cum_ibc_transfers
        """
    )
