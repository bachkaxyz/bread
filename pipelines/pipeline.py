import os
import argparse
import prefect
import asyncio
from extract import DataExtractor, get_min_height, get_max_height, get_min_ingested_height, get_max_ingested_height, get_min_height_from_files, get_max_height_from_files
from parse import DataParser
import subprocess

# Configuration
API_URL = os.getenv("API_URL")
NETWORK = os.getenv("NETWORK")
PER_PAGE = int(os.getenv("PER_PAGE", 100))
DATA_PATH = "./data"
MAX_SEMAPHORE = 10

@prefect.task(
    name="determine_sync_range",
    description="Determine the block range for syncing.",
)
def determine_sync_range(directory: str, num_blocks: int) -> tuple[int, int]:
    """
    Determine the block range for syncing the most recent data.
    """
    api_url = os.getenv("API_URL")
    
    # Retrieve the maximum and minimum block heights from the API
    max_node_height = get_max_height(api_url)
    min_node_height = get_min_height(api_url)

    # Retrieve the earliest and latest block heights we've already ingested
    min_ingested_height = get_min_height_from_files(directory)

    # Determine the (most recent) height to end sync with
    end_height = max_node_height

    # Determine the earliest height to sync to.
    # This considers the min ingested height and the max number of blocks we want to fetch in one go
    # Also, ensures we don't go beyond the min height provided by the API
    start_height = max(end_height - num_blocks, min_ingested_height + 1, min_node_height)
    
    return start_height, end_height


@prefect.task(
        name="determine_backfill_range",
        description="Determine the backfill range.",
)
def determine_backfill_range(directory: str, num_blocks: int) -> tuple[int, int]:
    """
    Determine the backfill range, syncing backwards from the last known block.
    """
    min_node_height = get_min_height(os.getenv("API_URL"))
    min_ingested_height = get_min_height_from_files(directory)

    # Set the end height for backfilling to the earliest block we have ingested minus one.
    # This is to avoid re-ingesting the last block we have.
    end_height = min_ingested_height - 1

    # Start the backfill from the block 'num_blocks' behind the end height, but ensure it's not less than the node's min height.
    start_height = max(end_height - num_blocks + 1, min_node_height)

    return start_height, end_height

@prefect.task(
    name="extract_data",
    description="Extract data from the RPC endpoints.",
)
def extract_data(start_height: int, end_height: int) -> None:
    api_url = os.getenv("API_URL")
    network = os.getenv("NETWORK")
    extractor = DataExtractor(
        api_url=api_url,
        start_height=start_height,
        end_height=end_height,
        per_page=int(os.getenv("PER_PAGE", 100)),
        protocol="rpc",
        network=network,
        semaphore=10)
    asyncio.run(extractor.async_extract())

@prefect.task(
    name="parse_data",
    description="Parse the extracted data into parquet files.",
)
def parse_data(data_path: str) -> str:
    network = os.getenv("NETWORK")
    parser = DataParser(blocks_path=f"./data/{network}/rpc/blocks",
                        txs_path=f"./data/{network}/rpc/txs",
                        output_path=f"./data/{network}/parsed")
    parser.run()

@prefect.task(
    name="run_makefile",
    description="Run a Makefile command.",
)
def run_makefile(command: str) -> str:
    return subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode("utf-8")

def read_error_heights_from_file(directory: str) -> list[int]:
    """Read the list of error heights from the file."""
    with open(directory, 'r') as f:
        # Convert each line to an integer and return as a list
        return [int(line.strip()) for line in f]
    
@prefect.task
def determine_gap_fill_range(directory) -> list[tuple[int, int]]:
    """Determine the gaps based on the error heights."""
    error_heights = read_error_heights_from_file(f"{directory}/errors/error_heights.txt")
    return error_heights

@prefect.flow(
    name="data_pipeline",
    description="A pipeline to extract, parse, and load data.",
)
def data_pipeline(directory: str, num_blocks: int = 10000):
    # Sync the most recent data
    sync_start, sync_end = determine_sync_range(f'{directory}/rpc/blocks', num_blocks)
    extract_data(sync_start, sync_end)

    # Backfill missing data from errors.json
    # gap_fill_ranges = determine_gap_fill_range(directory)
    # for gap_start, gap_end in gap_fill_ranges:
    #     extract_data(gap_start, gap_end)

    backfill_start, backfill_end = determine_backfill_range(f'{directory}/rpc/blocks', num_blocks)
    while backfill_start < backfill_end:
        extract_data(backfill_start, min(backfill_start + num_blocks, backfill_end))
        backfill_start += num_blocks + 1

    parse_data(f'{directory}/parsed')
    run_makefile('make dbt-run')

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Run the data extraction and processing flow.')   
    parser.add_argument('--dir', type=str, default=f'./data/{os.getenv("NETWORK")}', help='Directory of blocks') 
    parser.add_argument('--num_blocks', type=int, default=10000, help='Number of blocks to sync.')
    args = parser.parse_args()

    result = data_pipeline._run(directory=args.dir, num_blocks=args.num_blocks)