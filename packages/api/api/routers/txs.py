from api.database import database
from fastapi import APIRouter

from api.database import DBT_SCHEMA

router = APIRouter(
    prefix="/txs", tags=["txs"], responses={404: {"description": "Not found"}}
)


@router.get("/cumulative")
async def get_cumulative_txs():
    return await database.fetch_all(
        f"""
        select *
        from {DBT_SCHEMA}.cum_txs_per_day
        """
    )


@router.get("/daily")
async def daily_txs():
    return await database.fetch_all(
        f"""
        select *
        from {DBT_SCHEMA}.num_txs_per_day
        """
    )


@router.get("/gas")
async def get_gas_used_per_day():
    return await database.fetch_all(
        f"""
        select *
        from {DBT_SCHEMA}.gas_used_per_day
        """
    )
