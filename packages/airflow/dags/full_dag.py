import datetime
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
    schedule_interval="* * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessPrices():

    @task
    def get_data():
        cg = CoinGeckoAPI()
        
        tickers = ['alpha-finance', 'matic-network', '0x', 'yflink', 'enjincoin', 'decentraland', 'basic-attention-token', 'tornado-cash', 'thorchain', 'aave', 'band-protocol', 'havven', 'kyber-network', 'dai', 'wrapped-bitcoin', 'maker', 'ocean-protocol', 'chainlink', 'tether', 'uniswap', 'true-usd', 'compound-governance-token', 'ethereum', 'yearn-finance', 'basis-cash', 'usd-coin', 'reserve-rights-token', 'sushi', 'defipulse-index', 'republic-protocol', 'renbtc', 'secret-erc20', 'secret-finance', 'binancecoin', 'binance-eth', 'binance-peg-polkadot', 'binance-peg-cardano', 'binance-peg-xrp', 'binance-peg-dogecoin', 'binance-peg-bitcoin-cash', 'binance-peg-litecoin', 'binance-usd', 'tron-bsc', 'pancakeswap-token', 'bakerytoken', 'venus', 'lina', 'refinable', 'bunnycoin', 'sienna-erc20', 'monero', 'cosmos', 'osmosis', 'terra-luna', 'sentinel', 'secret', 'terrausd', 'akash-network', 'terra-krw', 'juno-network', 'chihuahua-token']
        str_tickers = ','.join(tickers)
        response = cg.get_price(ids=str_tickers, vs_currencies='usd')
        print(response)


    get_data()


dag = ProcessPrices()
