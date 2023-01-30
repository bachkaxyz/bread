from typing import Dict, List


class CosmosAPI:
    url: str
    archive: bool = False


chain_mapping: Dict[str, List[CosmosAPI]] = {
    "secret-4": [
        CosmosAPI(url="https://secret-4.api.trivium.network:1317"),
        CosmosAPI(url="https://scrt-lcd.blockpane.com", archive=True),
    ],
    "jackal-1": [
        CosmosAPI(url="https://api.jackalprotocol.com"),
        CosmosAPI(url="https://jackal-api.polkachu.com"),
        CosmosAPI(url=" https://api.jackal.nodestake.top"),
    ],
}
