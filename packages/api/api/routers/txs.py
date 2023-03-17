from api.database import database
from fastapi import APIRouter

router = APIRouter(
    prefix="/txs", tags=["txs"], responses={404: {"description": "Not found"}}
)


@router.get("/cumulative")
async def get_cumulative_txs():
    return await database.fetch_all(
        """
        with num_txs_per_day as (
            select count(1), date_trunc('day', timestamp) as d
            from txs
            group by d
            order by d
        ) 

        select
        d as "day",
        sum(count) over (order by d asc rows between unbounded preceding and current row) as "tx_count"
        from num_txs_per_day
        """
    )


@router.get("/daily")
async def daily_txs():
    return await database.fetch_all(
        """
        select date_trunc('day', timestamp) as "day", count(1) as "tx_count"
        from txs
        group by "day"
        order by "day"
        """
    )
