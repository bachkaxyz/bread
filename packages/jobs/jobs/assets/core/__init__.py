from dagster import asset, op
import requests


@asset  # (key_prefix=["jackal"], group_name="validator_set")
def get_validator_set_latest():
    res = requests.get(
        "https://api.jackalprotocol.com/cosmos/base/tendermint/v1beta1/validatorsets/latest"
    ).json()
    return res


# @asset
# def convert_validator_set_to_dataframe(get_validator_set_latest):
#     pass
