from dataclasses import dataclass
import os
from aiohttp import ClientSession
from typing import Any, Dict
from indexer.chain import CosmosChain, get_chain_from_environment


class Config:
    db_kwargs: Dict[str, str | Dict[str, str] | int]
    schema_name: str
    session_kwargs: Dict[str, Any]

    DROP_TABLES_ON_STARTUP: bool
    CREATE_TABLES_ON_STARTUP: bool

    USE_LOG_FILE: bool

    BUCKET_NAME: str

    chain: CosmosChain

    ENVIRONMENT: str

    def __init__(self):
        self.DROP_TABLES_ON_STARTUP = (
            os.getenv("DROP_TABLES_ON_STARTUP", "False").upper() == "TRUE"
        )
        self.CREATE_TABLES_ON_STARTUP = (
            os.getenv("CREATE_TABLES_ON_STARTUP", "false").upper() == "TRUE"
        )
        self.USE_LOG_FILE = os.getenv("USE_LOG_FILE", "TRUE").upper() == "TRUE"

        self.schema_name = os.getenv("INDEXER_SCHEMA", "public")
        self.db_kwargs = dict(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            database=os.getenv("POSTGRES_DB", "postgres"),
            server_settings={"search_path": self.schema_name},
            command_timeout=60,
        )
        self.session_kwargs = dict()

        self.BUCKET_NAME = os.getenv("BUCKET_NAME", "sn-mono-indexer-dev")

        self.ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

    async def configure(self):
        async with ClientSession() as session:
            self.chain = await get_chain_from_environment(session)
