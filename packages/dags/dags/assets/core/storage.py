import asyncio
from typing import List, Tuple
from asyncpg import Connection, Pool
from dagster import asset
import pandas as pd
import requests
from aiohttp import ClientSession

from dags.resources.postgres_resource import PostgresResource


@asset(group_name="jackal_providers")
def current_providers():
    j = requests.get(
        "https://jackal-rest.brocha.in/jackal-dao/canine-chain/storage/providers?pagination.limit=1000"
    ).json()
    providers = j["providers"]
    df = pd.DataFrame(providers)
    df["timestamp"] = pd.to_datetime("now")
    df = df.astype({"totalspace": int})
    print(df)
    return df


@asset(group_name="jackal_providers")
async def detailed_providers(current_providers: pd.DataFrame):
    async def get_addr_freespace(session: ClientSession, addr: str) -> Tuple[str, dict]:
        async with session.get(
            url=f"https://jackal-rest.brocha.in/jackal-dao/canine-chain/storage/freespace/{addr}"
        ) as resp:
            return (addr, await resp.json())

    async with ClientSession() as session:
        results = await asyncio.gather(
            *[
                get_addr_freespace(session, address)
                for address in current_providers["address"]
            ]
        )

    free_space = []
    for addr, res in results:
        if "space" in res:
            free_space.append((addr, int(res["space"])))

    freespace_df = pd.DataFrame(free_space, columns=["address", "freespace"])

    current_providers = current_providers.merge(freespace_df, on="address")
    current_providers["used_space"] = (
        current_providers["totalspace"] - current_providers["freespace"]
    )
    return current_providers


@asset(group_name="jackal_providers", required_resource_keys={"postgres"})
async def save_providers(
    context,
    detailed_providers: pd.DataFrame,
):
    pool: Pool = context.resources.postgres._pool
    async with pool.acquire() as conn:
        conn: Connection
        tuples = [tuple(x) for x in detailed_providers.values]

        s = await conn.copy_records_to_table(
            table_name="providers",
            records=tuples,
            columns=list(detailed_providers.columns),
            timeout=10,
        )
