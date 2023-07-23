import argparse
import prefect
import asyncio
from extract import DataExtractor, get_min_height, get_max_height, get_min_ingested_height, get_max_ingested_height
from parse import DataParser
import os
import subprocess


@prefect.task(
    name="determine_height",
    description="Determine the start and end height for data extraction.",
)
def determine_height(data_path: str) -> tuple[int, int]:
    api_url = os.getenv("API_URL")
    if api_url is None:
        print("API_URL environment variable is not set.")
        return (0, 0)  # return a default value

    min_node_height = get_min_height(api_url)
    max_node_height = get_max_height(api_url)

    min_ingested_height = get_min_ingested_height(data_path)
    max_ingested_height = get_max_ingested_height(data_path)

    if min_ingested_height == 0 or min_ingested_height > min_node_height:
        start_height = min_node_height
    else:
        start_height = max_ingested_height + 1

    if max_node_height - start_height > 10000:
        end_height = start_height + 9999
    else:
        end_height = max_node_height

    return (start_height, end_height)



@prefect.task(
    name="extract_data",
    description="Extract data from the RPC endpoints.",
)
def extract_data(heights: tuple[int, int], data_path: str) -> str:
    api_url = os.getenv("API_URL")
    network = os.getenv("NETWORK")
    extractor = DataExtractor(api_url=api_url,
                              start_height=heights[0],
                              end_height=heights[1] - 1,
                              per_page=int(os.getenv("PER_PAGE", 100)),
                              protocol="rpc",
                              network=network,
                              semaphore=10)
    asyncio.run(extractor.async_extract())
    return data_path


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
    return f"./data/{network}/parsed"


@prefect.flow(
    name="data_pull",
    description="A pipeline to just pull data from the RPC endpoints.",
)
def data_pull():
    data_path = "./data"
    heights = determine_height(data_path)
    raw_data = extract_data(heights, data_path)

@prefect.task(
    name="run_makefile",
    description="Run a Makefile command.",
)
def run_makefile(command: str) -> str:
    return subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode("utf-8")
@prefect.flow(
    name="data_pipeline",
    description="A pipeline to extract, parse, and load data.",
)
def data_pipeline():
    data_path = "./data"
    heights = determine_height(data_path)
    raw_data = extract_data(heights, data_path)
    parsed_data = parse_data(raw_data)
    run_makefile('make dbt-run')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run a pipeline.')
    parser.add_argument('--pipeline', type=str, default='full',
                        help='Which pipeline to run: "full" or "pull". Default is "full".')
    args = parser.parse_args()

    if args.pipeline == 'full':
        result = data_pipeline._run()
    elif args.pipeline == 'pull':
        result = data_pull._run()
    else:
        print(f'Invalid pipeline: {args.pipeline}. Choose "full" or "pull".')