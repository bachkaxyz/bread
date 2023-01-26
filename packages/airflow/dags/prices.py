import datetime
import time
import pendulum

import pandas as pd
from pycoingecko import CoinGeckoAPI
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="process-prices",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessPrices():
    token_mapper = {
        "tether": "USDT",
        "true-usd": "TUSD",
        "ethereum": "ETH",
        "usd-coin": "USDC",
        "secret-erc20": "wSCRT",
        "secret-finance": "SEFI",
        "binance-usd": "BUSD",
        "sienna": "SIENNA",
        "cosmos": "ATOM",
        "osmosis": "OSMO",
        "secret": "SCRT",
        "terrausd": "UST",
        "juno-network": "JUNO",
    }

    ninety_day_seconds = datetime.timedelta(days=90).total_seconds()

    @task()
    def get_min_time():
        try:
            postgres = PostgresHook(postgres_conn_id="workhorse")
            max_time = postgres.get_first("select max(time) from prices")[0]
        except:
            max_time = None
        if max_time is None:
            return 1664042513
        return (
            int(max_time.strftime("%s")) + 3600
        )  # converts to unix timestamp and add 1 hour to prevent primary key violation

    @task()
    def get_price_data(ticker: str, min_time: int, cur_time: int):
        cg = CoinGeckoAPI()
        ticker_prices = []
        print("processing ticker: ", ticker)
        next_time = min_time
        # for hourly data we need to get 90 days at a time
        while next_time < cur_time:
            tries = 0
            data = None
            print(
                "processing time: ",
                datetime.datetime.fromtimestamp(next_time),
                " to ",
                datetime.datetime.fromtimestamp(next_time + ninety_day_seconds),
                "try #",
                tries,
            )
            while data is None:
                try:
                    data = cg.get_coin_market_chart_range_by_id(
                        ticker,
                        vs_currency="usd",
                        from_timestamp=min_time,
                        to_timestamp=next_time + ninety_day_seconds,
                    )
                except Exception as e:
                    print("error: ", e)
                    time.sleep(20)

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

        prices_df = (
            pd.concat(ticker_prices, axis=1)
            .sort_index()
            .ffill()
            .drop_duplicates()
            .resample("1H")
            .last()
            .ffill()
        )
        print(prices_df.head(), prices_df.tail())
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
        print(df)
        print([list(i) for i in df.itertuples()])
        postgres = PostgresHook(postgres_conn_id="workhorse")
        postgres.insert_rows(
            table="prices",
            rows=[list(i) for i in df.itertuples()],
            commit_every=1000,
            replace=True,
            replace_index=["time"],
            target_fields=["time"] + [f'"{col}"' for col in df.columns.tolist()],
        )
        print("appended new rows")

    insert_data(prices_df_json)


dag = ProcessPrices()
