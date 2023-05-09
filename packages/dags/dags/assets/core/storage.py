import asyncio
from typing import List, Tuple
from asyncpg import Connection, Pool
from dagster import OpExecutionContext, asset
import pandas as pd
import requests
from aiohttp import ClientSession

from dags.resources.postgres_resource import PostgresResource

GROUP_NAME = "jackal_providers"
KEY_PREFIX = "jackal_providers"


@asset(group_name=GROUP_NAME, key_prefix=KEY_PREFIX)
def current_providers():
    j = requests.get(
        "https://jackal-rest.brocha.in/jackal-dao/canine-chain/storage/providers?pagination.limit=1000"
    ).json()
    providers = j["providers"]
    df = pd.DataFrame(providers)
    df = df.astype({"totalspace": int})
    print(df)
    return df


@asset(group_name=GROUP_NAME, key_prefix=KEY_PREFIX)
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

    current_providers["freespace"] = current_providers["freespace"] / 10**9

    current_providers["totalspace"] = current_providers["totalspace"] / 10**9
    return current_providers


@asset(
    group_name=GROUP_NAME, key_prefix=KEY_PREFIX, required_resource_keys={"postgres"}
)
async def create_providers_table(context: OpExecutionContext):
    postgres: PostgresResource = context.resources.postgres
    conn: Connection = await postgres.get_conn()
    print(postgres.s)
    query = f"""
        CREATE TABLE IF NOT EXISTS {str(postgres.s)}.providers (
            address TEXT,
            creator TEXT,
            ip TEXT,
            burned_contracts TEXT,
            keybase_identity TEXT,
            auth_claimers TEXT[],
            totalspace decimal,
            freespace decimal,
            timestamp TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (address, timestamp)
            
        );
        """
    print(query)
    await conn.execute(query)
    await conn.close()


@asset(
    group_name=GROUP_NAME,
    key_prefix=KEY_PREFIX,
    required_resource_keys={"postgres"},
    non_argument_deps={"create_providers_table"},
)
async def providers(
    context: OpExecutionContext,
    detailed_providers: pd.DataFrame,
):
    postgres: PostgresResource = context.resources.postgres
    conn: Connection = await postgres.get_conn()

    tuples = [tuple(x) for x in detailed_providers.values]

    await conn.copy_records_to_table(
        table_name="providers",
        schema_name=str(postgres.s),
        records=tuples,
        columns=list(detailed_providers.columns),
        timeout=10,
    )

    await conn.close()
