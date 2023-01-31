from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class CosmosChain:
    chain_id: str
    min_block_height: int
    apis: List["CosmosAPI"] = field(default_factory=list)


@dataclass
class CosmosAPI:
    url: str
    archive: bool = False


chain_mapping: List[CosmosChain] = [
    CosmosChain(
        chain_id="secret-4",
        min_block_height=6955001,
        apis=[
            CosmosAPI(url="https://secret-4.api.trivium.network:1317"),
        ],
    ),
    CosmosChain(
        chain_id="jackal-1",
        min_block_height=1,
        apis=[
            CosmosAPI(url="https://api.jackalprotocol.com"),
            CosmosAPI(url="https://jackal-api.polkachu.com"),
            CosmosAPI(url=" https://api.jackal.nodestake.top"),
        ],
    ),
    CosmosChain(
        chain_id="akashnet-2",
        min_block_height=9262196,
        apis=[CosmosAPI(url="https://akash-api.polkachu.com")],
    ),
]
