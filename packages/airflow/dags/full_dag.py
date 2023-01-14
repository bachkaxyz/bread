import asyncio
import datetime
import time
import pendulum
import os

import requests
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
    tickers = [
        "alpha-finance",
        "matic-network",
        # "0x",
        # "yflink",
        # "enjincoin",
        # "decentraland",
        # "basic-attention-token",
        # "tornado-cash",
        # "thorchain",
        # "aave",
        # "band-protocol",
        # "havven",
        # "kyber-network",
        # "dai",
        # "wrapped-bitcoin",
        # "maker",
        # "ocean-protocol",
        # "chainlink",
        # "tether",
        # "uniswap",
        # "true-usd",
        # "compound-governance-token",
        # "ethereum",
        # "yearn-finance",
        # "basis-cash",
        # "usd-coin",
        # "reserve-rights-token",
        # "sushi",
        # "defipulse-index",
        # "republic-protocol",
        # "renbtc",
        # "secret-erc20",
        # "secret-finance",
        # "binancecoin",
        # "binance-eth",
        # "binance-peg-polkadot",
        # "binance-peg-cardano",
        # "binance-peg-xrp",
        # "binance-peg-dogecoin",
        # "binance-peg-bitcoin-cash",
        # "binance-peg-litecoin",
        # "binance-usd",
        # "tron-bsc",
        # "pancakeswap-token",
        # "bakerytoken",
        # "venus",
        # "lina",
        # "refinable",
        # "bunnycoin",
        # "sienna-erc20",
        # "monero",
        # "cosmos",
        # "osmosis",
        # "terra-luna",
        # "sentinel",
        # "secret",
        # "terrausd",
        # "akash-network",
        # "terra-krw",
        # "juno-network",
        # "chihuahua-token",
    ]

    ninety_day_seconds = datetime.timedelta(days=90).total_seconds()

    @task()
    def get_min_time():
        return 1607957730

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
            per_ticker_df = pd.DataFrame(prices, columns=["time", ticker])
            per_ticker_df["time"] = pd.to_datetime(per_ticker_df.time, unit="ms")
            per_ticker_df.set_index("time", inplace=True)
            ticker_prices.append(per_ticker_df)
        prices_df = pd.concat(ticker_prices).sort_index()
        return prices_df.to_json()

    @task()
    def append_to_postgres(data):
        pass

    min_time = get_min_time()
    cur_time = time.time()

    data = get_price_data.partial(min_time=min_time, cur_time=cur_time).expand(
        ticker=tickers,
    )

    prices_df_json = normalize_price_data(data)


dag = ProcessPrices()
