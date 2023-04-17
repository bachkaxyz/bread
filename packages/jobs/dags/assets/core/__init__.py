import pandas as pd
from dagster import asset, op
from pycoingecko import CoinGeckoAPI
from sqlalchemy import create_engine


@asset(required_resource_keys={"postgres"}, key_prefix=["current_price"])
def create_coin_gecko_id_table(context):
    engine = create_engine(context.resources.postgres._con)
    conn = engine.connect()
    conn = conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {context.resources.postgres._schema}.coin_gecko_ids (
            id TEXT PRIMARY KEY
        );
        """,
    )
    conn.close()
    engine.dispose()


@asset(
    required_resource_keys={"postgres"},
    non_argument_deps={"create_coin_gecko_id_table"},
    key_prefix=["current_price"],
)
def load_coin_gecko_ids(context):
    engine = create_engine(context.resources.postgres._con)
    conn = engine.connect()
    conn = conn.execute(
        f"SELECT id FROM {context.resources.postgres._schema}.coin_gecko_ids;"
    )
    results = conn.fetchall()
    return results


@asset(io_manager_key="postgres", key_prefix=["current_price"])
def get_current_price(load_coin_gecko_ids):
    cg = CoinGeckoAPI()
    res = []
    for id in load_coin_gecko_ids:
        res.append(
            cg.get_price(
                ids="jackal-protocol",
                vs_currencies="usd",
                include_market_cap="true",
                include_24hr_vol="true",
                include_24hr_change="true",
                include_last_updated_at="true",
            )
        )

    df = pd.DataFrame.from_records(res)
    df = df.T
    df["time"] = pd.to_datetime(df["last_updated_at"], unit="ms")
    df.drop(columns=["last_updated_at"], inplace=True)
    df.reset_index(inplace=True)
    df.columns = ["id", "price", "market_cap", "volume", "change", "time"]

    return df
