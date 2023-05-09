from typing import List
from asyncpg import Connection, Pool
import pandas as pd
from dagster import asset, op
from pycoingecko import CoinGeckoAPI

from dags.resources.postgres_resource import PostgresResource


@asset(required_resource_keys={"postgres"}, group_name="current_price")
def create_coin_gecko_id_table(context):
    postgres = context.resources.postgres
    conn = postgres._get_conn()
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {context.resources.postgres._schema}.coin_gecko_ids (
            id TEXT PRIMARY KEY
        );
        """
    )
    conn.close()


@asset(
    required_resource_keys={"postgres"},
    non_argument_deps={"create_coin_gecko_id_table"},
    group_name="current_price",
)
async def load_coin_gecko_ids(context) -> List[str]:
    postgres: PostgresResource = context.resources.postgres
    conn: Connection = await postgres.get_conn()
    results = await conn.fetch(
        f"SELECT id FROM {context.resources.postgres._schema}.coin_gecko_ids;"
    )
    conn.close()
    print(results)
    res = [result[0] for result in results]
    print(res)
    return res  # type: ignore


@asset(group_name="current_price")
def get_current_prices(load_coin_gecko_ids):
    cg = CoinGeckoAPI()
    res = []
    ids = ",".join(load_coin_gecko_ids)
    print(ids)
    response = cg.get_price(
        ids=ids,
        vs_currencies="usd",
        include_market_cap="true",
        include_24hr_vol="true",
        include_24hr_change="true",
        include_last_updated_at="true",
    )
    print(response[load_coin_gecko_ids[0]])
    res = [[id] + list(response[id].values()) for id in load_coin_gecko_ids]
    print(res)
    df = pd.DataFrame.from_records(
        res,
        columns=[
            "id",
            "price",
            "market_cap",
            "24h_volume",
            "24hr_change",
            "last_updated_at",
        ],
    )
    print(df)

    df["time"] = pd.to_datetime(df["last_updated_at"], unit="ms")
    df.drop(columns=["last_updated_at"], inplace=True)

    return df


# need to save output to postgres
@asset(group_name="current_price")
async def save_prices_to_postgres(get_current_prices: pd.DataFrame):
    pass
