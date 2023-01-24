import datetime
import time
import pendulum

import pandas as pd
from pycoingecko import CoinGeckoAPI
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


@dag(
    dag_id="validator_distribution",
    schedule="* 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ValidatorDistribution():

    sn_lcd_api = "https://lcd.spartanapi.dev"

    @task()
    def get_validators():
        response = requests.get(
            f"{sn_lcd_api}/cosmos/staking/v1beta1/validators?pagination.limit=500&pagination.count_total=true"
        )
        return response.json()["validators"]

    @task()
    def parse_all_validators(all_validators):
        data = []
        for val in all_validators:
            data.append(
                (
                    val["operator_address"],
                    val["description"]["moniker"],
                    val["delegator_shares"],
                )
            )
        return data

    @task()
    def get_all_delegations(parsed_validator_data):
        with_delegations = []

        for i, (addr, mon, shares) in enumerate(parsed_validator_data):
            response = requests.get(
                f"{sn_lcd_api}/cosmos/staking/v1beta1/validators/{addr}/delegations?pagination.limit=10000000"
            )
            try:
                num_delegations = len(response.json()["delegation_responses"])
                with_delegations.append((addr, mon, shares, num_delegations))
                print(f"validator {mon} has {num_delegations} delegations")
            except:
                print(f"validator {mon} failed")
                print(response.json())

        return with_delegations

    @task()
    def save_to_db(delegations_data):
        postgres = PostgresHook(postgres_conn_id="workhorse")
        df = pd.DataFrame(
            delegations_data, columns=["address", "moniker", "shares", "delegators"]
        )
        df.set_index("address", inplace=True)
        print(df)
        inserts = list(df.itertuples(index=True))
        print(inserts)
        postgres.insert_rows(
            table="validator_distribution",
            rows=inserts,
            replace=True,
            target_fields=[df.index.name] + df.columns.tolist(),
            replace_index=["address"],
        )

    all_validators = get_validators()
    parsed_data = parse_all_validators(all_validators)
    delegations_data = get_all_delegations(parsed_data)
    save_to_db(delegations_data)


dag = ValidatorDistribution()
