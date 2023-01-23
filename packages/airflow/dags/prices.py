import datetime
import time
import pendulum

import pandas as pd
from pycoingecko import CoinGeckoAPI
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="process-prices",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessPrices():
    token_mapper = {
        "alpha-finance": "ALPHA",
        "matic-network": "MATIC",
        # "0x": "ZRX",
        # "yflink": "YFL",
        # "enjincoin": "ENJ",
        # "decentraland": "MANA",
        # "basic-attention-token": "BAT",
        # "tornado-cash": "TORN",
        # "thorchain": "RUNE",
        # "aave": "AAVE",
        # "band-protocol": "BAND",
        # "havven": "SNX",
        # "kyber-network": "KNC",
        # "dai": "DAI",
        # "wrapped-bitcoin": "WBTC",
        # "maker": "MKR",
        # "ocean-protocol": "OCEAN",
        # "chainlink": "LINK",
        # "tether": "USDT",
        # "uniswap": "UNI",
        # "true-usd": "TUSD",
        # "compound-governance-token": "COMP",
        # "ethereum": "ETH",
        # "yearn-finance": "YFI",
        # "basis-cash": "BAC",
        # "usd-coin": "USDC",
        # "reserve-rights-token": "RSR",
        # "sushi": "SUSHI",
        # "defipulse-index": "DPI",
        # "republic-protocol": "REN",
        # "renbtc": "renBTC",
        # "secret-erc20": "wSCRT",
        # "secret-finance": "SEFI",
        # "binancecoin": "BNB",
        # "binance-eth": "ETH",
        # "binance-peg-polkadot": "DOT",
        # "tether": "USDT",
        # "binance-peg-cardano": "ADA",
        # "binance-peg-xrp": "XRP",
        # "binance-peg-dogecoin": "DOGE",
        # "usd-coin": "USDC",
        # "binance-peg-bitcoin-cash": "BCH",
        # "binance-peg-litecoin": "LTC",
        # "binance-usd": "BUSD",
        # "tron-bsc": "TRX",
        # "pancakeswap-token": "CAKE",
        # "bakerytoken": "BAKE",
        # "venus": "XVS",
        # "lina": "LINA",
        # "refinable": "FINE",
        # "bunnycoin": "BUNNY",
        # "sienna-erc20": "wSIENNA",
        # "monero": "XMR",
        # "cosmos": "ATOM",
        # "osmosis": "OSMO",
        # "terra-luna": "LUNA",
        # "sentinel": "DVPN",
        # "secret": "SCRT",
        # "terrausd": "UST",
        # "akash-network": "AKT",
        # "terra-krw": "KRW",
        # "juno-network": "JUNO",
        # "chihuahua-token": "HUAHUA",
    }

    ninety_day_seconds = datetime.timedelta(days=90).total_seconds()

    @task()
    def get_min_time():
        # return 1607957730
        return 1663958551

    @task()
    def get_price_data(ticker: str, min_time: int, cur_time: int):
        cg = CoinGeckoAPI()
        ticker_prices = []
        print("processing ticker: ", ticker)
        next_time = min_time
        # for hourly data we need to get 90 days at a time
        while next_time < cur_time:
            print(
                "processing time: ",
                datetime.datetime.fromtimestamp(next_time),
                " to ",
                datetime.datetime.fromtimestamp(next_time + ninety_day_seconds),
            )
            data = cg.get_coin_market_chart_range_by_id(
                ticker,
                vs_currency="usd",
                from_timestamp=min_time,
                to_timestamp=next_time + ninety_day_seconds,
            )
            next_time += ninety_day_seconds
            ticker_prices.extend(data["prices"])
        return ticker, ticker_prices

    @task()
    def normalize_price_data(data):
        ticker_prices = []
        for ticker, prices in data:
            per_ticker_df = pd.DataFrame(prices, columns=["time", token_mapper[ticker]])
            per_ticker_df["time"] = pd.to_datetime(per_ticker_df.time, unit="ms")
            per_ticker_df.drop_duplicates()
            per_ticker_df.set_index("time", inplace=True)
            per_ticker_df = per_ticker_df.resample("H").last().ffill()
            ticker_prices.append(per_ticker_df)

        prices_df = pd.concat(ticker_prices, axis=1)
        print(prices_df.to_json())
        return prices_df.to_json()

    min_time = get_min_time()
    cur_time = time.time()

    data = get_price_data.partial(min_time=min_time, cur_time=cur_time).expand(
        ticker=list(token_mapper.keys()),
    )

    prices_df_json = normalize_price_data(data)

    @task()
    def insert_data(data):
        df = pd.read_json(data)
        inserts = list(df.itertuples())
        print(inserts[0])
        postgres = PostgresHook(postgres_conn_id="workhorse")
        postgres.insert_rows(table="prices", rows=inserts, commit_every=1000)

    insert_data(prices_df_json)


dag = ProcessPrices()
