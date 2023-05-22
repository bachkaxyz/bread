import logging
from aiohttp import ClientSession
from indexer.chain import CosmosChain

from parse import Raw


async def process_tx(
    raw: Raw, session: ClientSession, chain: CosmosChain
) -> Raw | None:
    """Query and process transactions from raw block

    Args:
        raw (Raw): Raw block to process transactions for
        session (ClientSession): Client session to use for querying
        chain (CosmosChain): Chain to query for transactions

    Returns:
        (Raw): Raw block
    """
    logger = logging.getLogger("indexer")
    # these are the fields required to process transactions
    if raw.height is not None and raw.block_tx_count is not None:
        tx_res_json = await chain.get_block_txs(
            session=session,
            height=raw.height,
        )
        # check that transactions exist
        if tx_res_json is not None and "tx_responses" in tx_res_json:
            tx_responses = tx_res_json["tx_responses"]
            raw.parse_tx_responses(tx_responses)
            logger.info(
                f"{raw.height=} {raw.tx_responses_tx_count=} {raw.block_tx_count=} {len(tx_responses)=}"
            )
            # check that the number of transactions in the block matches the number of transactions in the tx_responses
            if raw.block_tx_count == raw.tx_responses_tx_count:
                return raw
            else:
                logger.info(
                    f"process - {raw.height} - tx count of {raw.tx_responses_tx_count=} {raw.block_tx_count=} {len(tx_responses)=} not right "
                )
                return Raw(
                    height=raw.height,
                    chain_id=raw.chain_id,
                    block_tx_count=raw.block_tx_count,
                    tx_responses_tx_count=None,
                    block=raw.block,
                    raw_block=raw.raw_block,
                )

        else:
            logger.info(
                f"process - {raw.height} - tx_response is not a key or tx_res_json is none"
            )
            return Raw(
                height=raw.height,
                chain_id=raw.chain_id,
                block_tx_count=raw.block_tx_count,
                tx_responses_tx_count=None,
                block=raw.block,
                raw_block=raw.raw_block,
            )
    else:
        logger.error(
            f"process - {raw.height} - raw.height or raw.block_tx_count does not exist so cannot parse txs"
        )
        return None


async def process_block(
    block_raw_data: dict, session: ClientSession, chain: CosmosChain
) -> Raw | None:
    """Processes the raw block data and returns a Raw object

    Args:
        block_raw_data (dict): Block data to process from the chain
        session (ClientSession): Client session to use for requests
        chain (CosmosChain): CosmosChain object to use for requests

    Returns:
        Raw: Raw object with the processed data
    """
    raw = Raw()
    raw.parse_block(block_raw_data)
    logger = logging.getLogger("indexer")

    # block unsuccessfully parsed
    if raw.height is None or raw.block_tx_count is None:
        logger.info("block not parsed")
        return None

    # block and transactions successfully parsed with transactions
    if raw.height and raw.block_tx_count and raw.block_tx_count > 0:
        logger.info("raw block tx count > 0")
        return await process_tx(raw, session, chain)

    # block successfully parsed but no transactions
    if raw.height and raw.block_tx_count == 0:
        logger.info("raw block tx count == 0")
        return Raw(
            height=raw.height,
            chain_id=raw.chain_id,
            block_tx_count=raw.block_tx_count,
            tx_responses_tx_count=0,
            block=raw.block,
            raw_block=raw.raw_block,
        )
