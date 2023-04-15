from api.database import database
from fastapi import APIRouter

router = APIRouter(
    prefix="/txs", tags=["txs"], responses={404: {"description": "Not found"}}
)


@router.get("/cumulative")
async def get_cumulative_txs():
    return await database.fetch_all(
        """
        select *
        from dbt.cum_txs_per_day
        """
    )


@router.get("/daily")
async def daily_txs():
    return await database.fetch_all(
        """
        select *
        from dbt.num_txs_per_day
        """
    )
