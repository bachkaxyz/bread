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
        select *
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
