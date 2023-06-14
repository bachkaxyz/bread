import json
import os
from indexer.chain import CosmosChain
from indexer.config import Config
from deepdiff import DeepDiff


async def test_config(mocker):
    mocker.patch(
        "os.environ",
        dict(
            GIT_BRANCH="jackal",
            DROP_TABLES_ON_STARTUP="true",
            CREATE_TABLES_ON_STARTUP="true",
            USE_LOG_FILE="false",
            CHAIN_REGISTRY_NAME="secretnetwork",
            LOAD_CHAIN_REGISTRY_APIS="false",
            CHAIN_ID="secret-4",
            BLOCKS_ENDPOINT="/cosmos/base/tendermint/v1beta1/blocks/{}",
            TXS_ENDPOINT="/cosmos/tx/v1beta1/txs?events=tx.height={}",
            APIS="https://lcd.secret.express",
            BATCH_SIZE="100",
            STEP_SIZE="20",
            TIME_BETWEEN_BLOCKS="1",
            POSTGRES_HOST="postgres",
            POSTGRES_PORT="5431",
            POSTGRES_DB="postgres",
            POSTGRES_USER="postgres",
        ),
    )
    config = Config()

    await config.configure()

    assert {} == DeepDiff(
        config.__dict__,
        {
            "DROP_TABLES_ON_STARTUP": True,
            "CREATE_TABLES_ON_STARTUP": True,
            "USE_LOG_FILE": False,
            "schema_name": "public",
            "db_kwargs": {
                "host": "postgres",
                "port": "5431",
                "user": "postgres",
                "password": "postgres",
                "database": "postgres",
                "server_settings": {"search_path": "public"},
                "command_timeout": 60,
            },
            "session_kwargs": {},
            "BUCKET_NAME": "sn-mono-indexer-dev",
            "chain": CosmosChain(
                chain_id="secret-4",
                chain_registry_name="secretnetwork",
                blocks_endpoint="/cosmos/base/tendermint/v1beta1/blocks/{}",
                txs_endpoint="/cosmos/tx/v1beta1/txs?events=tx.height={}",
                apis={"http://lcd.secret.express": {"hit": 0, "miss": 0, "times": []}},
                current_api_index=0,
                time_between_blocks=1,
                batch_size=100,
                step_size=20,
            ),
            "ENVIRONMENT": "development",
        },
    )

    mocker.resetall()
