from api.database import database
from fastapi import APIRouter

from api.database import DBT_SCHEMA

router = APIRouter(
    prefix="/storage",
    tags=["jackal"],
    responses={404: {"description": "Not found"}},
)


@router.get("/providers/network")
async def network_providers():
    print(DBT_SCHEMA)
    return await database.fetch_all(
        f"""
        select timestamp, usedspace / 1e3 as "Used Space", totalspace / 1e3 as "Total Space", freespace / 1e3 as "Free Space"
        from {DBT_SCHEMA}.network_space
        """
    )


@router.get("/providers")
async def individual_providers():
    return await database.fetch_all(
        f"""
        select *
        from {DBT_SCHEMA}.provider_space
        """
    )


@router.get("/providers/count")
async def count_of_providers():
    return await database.fetch_all(
        f"""
        select *
        from {DBT_SCHEMA}.count_providers
        """
    )


@router.get("/buys")
async def storage_buys():
    return await database.fetch_all(
        f"""
        select timestamp, transfer_amount / 1e6 as transfer_amount, message_sender
        from {DBT_SCHEMA}.buy_storage
        """
    )


@router.get("/rewards")
async def storage_rewards():
    return await database.fetch_all(
        f"""
        select *
        from {DBT_SCHEMA}.distribute_rewards
        """
    )


@router.get("/buys/cumulative")
async def storage_buys_cumulative():
    return await database.fetch_all(
        f"""
        select transfer_amount / 1e6 as transfer_amount, timestamp, message_sender
        from {DBT_SCHEMA}.cum_buy_storage
        """
    )
