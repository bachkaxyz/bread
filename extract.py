import requests
import time
import pandas as pd
from requests.exceptions import JSONDecodeError
import math
import os
import orjson
import glob

import aiohttp
import asyncio

class DataExtractor:
    """
    A class used to extract data from the Tendermint blockchain using the RPC endpoints.
    """

    def __init__(self, api_url, start_height, end_height, per_page, protocol, network, semaphore=1) -> None:
        """
        Initialize the DataExtractor object.

        Args:
            api_url (str): The base URL for the RPC API.
            start_height (int): The starting block height for the extraction.
            end_height (int): The ending block height for the extraction.
            per_page (int): The number of transactions to extract in each page.
            protocol (str): The protocol to be used ('rpc' or 'lcd').
            network (str): The name of the blockchain
        """
        self.api_url = api_url
        self.start_height = start_height
        self.end_height= end_height
        self.end_init = end_height
        self.start_init = start_height
        self.per_page_init = per_page
        self.per_page = per_page
        self.session = requests.Session() # use a session to keep the connection open
        self.protocol = protocol
        self.network = network
        self.semaphore = semaphore

    def query_rpc(self, endpoint_format: str, data_key: str):
        """
        Query the blockchain API.

        Args:
            endpoint_format (str): A format string for the API endpoint URL.
            data_key (str): The key in the response JSON where the data is stored.

        Returns:
            list: A list of data from the queried blocks or transactions.
        """
        data = []

        page = 1
        total_items_processed = 0
        remaining_pages_to_iterate = None

        while remaining_pages_to_iterate is None or page <= remaining_pages_to_iterate:
            print(f'page {page} with {remaining_pages_to_iterate} remaining pages to iterate for data keys: {data_key} | total processed: {total_items_processed}')
            self.per_page = self.per_page_init  # reset per_page to its initial value at the beginning of each loop
            endpoint = endpoint_format.format(api_url=self.api_url, start=self.start_height, end=self.end_height, page=page, per_page=self.per_page)
            while True: # retry loop
                try:
                    r = self.session.get(endpoint).json()
                    if 'result' in r.keys():
                        total_count = int(r['result']['total_count'])
                        remaining_pages_to_iterate = math.ceil(total_count / self.per_page)

                        # success, exit retry loop
                        new_data = r['result'][data_key]
                        data.extend(new_data)
                        total_items_processed += len(new_data)
                        break
                except (JSONDecodeError, KeyError) as e:
                    # Handle errors due to large response or missing 'result' key
                    if isinstance(e, JSONDecodeError):
                        print(f'Response too large. Reducing per_page from {self.per_page} to {self.per_page//2}')
                        self.per_page //= 2
                        remaing_pages_to_iterate = None # trying to update this in case the <= remaining_pages_to_iterate is fucked
                        if self.per_page < 1:
                            with open(f"./data/{self.network}/{self.protocol}/errors/error_heights.txt", "a") as error_file:
                                error_file.write(str(self.start_height) + "\n")
                            print("Warning: per_page has reached zero, can't reduce further. Moving to next block.")
                            return data

                        # Recalculate remaining pages with new per_page value
                        # remaining_pages_to_iterate = math.ceil(total_count / self.per_page)

                        # Recalculate the current page based on the total number of items processed
                        page = total_items_processed // self.per_page + 1
                        
                    else:  # KeyError
                        print(f"Unexpected response format, retrying. Error: {e}")
                    time.sleep(1)
            
            page += 1
            
        return data

    def query_api(self, endpoint_format: str, data_key: str):
        """
        Query the blockchain API.

        Args:
            endpoint_format (str): A format string for the API endpoint URL.
            data_key (str): The key in the response JSON where the data is stored.

        Returns:
            list: A list of data from the queried blocks or transactions.
        """
        self.per_page = self.per_page_init
        txs_data = []
        tx_responses_data = []

        page = 1
        remaining_pages_to_iterate = None

        while remaining_pages_to_iterate is None or page <= remaining_pages_to_iterate:
            endpoint = endpoint_format.format(api_url=self.api_url, start=self.start_height, end=self.end_height, page=page, per_page=self.per_page)
            r = self.session.get(endpoint).json()
            self.test = r
            remaining_pages_to_iterate = int(r['pagination']['total'])
            
            r.pop('pagination') # this info is just to get the counts, no longer needed, can skip it in future queries
            # Extend the appropriate lists with the data
            txs_data.extend(r['txs'])
            tx_responses_data.extend(r['tx_responses'])
            
            page += 1

        # Create DataFrame with txs_data and tx_responses_data as columns
        data = {'txs': txs_data, 'tx_responses': tx_responses_data}
            
        return data

    def query_txs(self, mode=None):
        """
        Wrapper for querying RPC vs. LCD. Note that RPC is ~50x faster for these style queries.
        """

        if self.protocol == 'rpc':
            txs = self.query_rpc(
                endpoint_format='{api_url}/tx_search?query="tx.height>={start} AND tx.height<={end}"&page={page}&per_page={per_page}&order_by="asc"&match_events=true',
                data_key='txs'
            )
            if mode == 'backfill':
                self.backfilled_txs.append(txs)
            else:
                self.txs = txs
                self.tx_df = pd.DataFrame(txs)

        elif self.protocol == 'lcd':
            txs = self.query_api(
                endpoint_format='{api_url}/cosmos/tx/v1beta1/txs?events=tx.height>={start}&events=tx.height<={end}&pagination.offset={page}&pagination.limit={per_page}&pagination.count_total=true&order_by=ORDER_BY_ASC',
                data_key=['txs', 'tx_responses']
            )
            self.txs = txs
            self.tx_df = pd.DataFrame(txs)

    def query_blocks(self):
        blocks = self.query_rpc(
            endpoint_format='{api_url}/block_search?query="block.height>={start} AND block.height<={end}"&page={page}&per_page={per_page}&order_by="asc"&match_events=true',
            data_key='blocks'
        )
        self.blocks = blocks
        self.blocks_df = pd.DataFrame(blocks)

    def save_json(self, data, prefix):
        """
        Save the raw JSON data to a file.

        Args:
            data (list): The data to save. This should be a list of dictionaries.
            prefix (str): The prefix of the filename.
        """
        # I'm assuming here that 'height' is a key in each dictionary in your data list. 
        # Adjust this if that's not the case.
       
        directory = f"/app/data/{self.network}/{self.protocol}/{prefix}"
        os.makedirs(directory, exist_ok=True)
        
        filename = f"{directory}/{self.start_height}_{self.end_height}.json"

        with open(filename, 'wb') as f:
            f.write(orjson.dumps(data))

    async def async_query_txs(self, blocks, session):
        """
        Asynchronously query transactions for a list of blocks.

        Args:
            blocks (list): The list of block heights to query.
            session (aiohttp.ClientSession): The session to use for the request.

        Returns:
            list: A list of transactions from the queried blocks.
        """
        tasks = [self.query_rpc(
            endpoint_format='{api_url}/tx_search?query="tx.height>={start} AND tx.height<={end}"&page={page}&per_page={per_page}&order_by="asc"&match_events=true',
            data_key='txs',
            session=session,
            block=block
        ) for block in blocks]
        results = await asyncio.gather(*tasks)
        return results

    async def backfill(self):
        """
        Backfill the DataFrame with missing blocks and transactions.

        This method is useful for when the DataFrame is saved to a file and needs to be updated.
        """
        # Create a dictionary where the key is the block height and the value is the number of transactions in that block
        blocks_and_txs = {x['header']['height']: len(x['data']['txs']) for x in self.blocks_df['block'].values}
        blocks_with_txs = {x:y for x,y in blocks_and_txs.items() if y > 0}

        # Get the total number of transactions logged
        num_txs_logged = self.tx_df.shape[0]

        # Get the total number of transactions from the blocks
        total_txs_from_blocks = sum(blocks_and_txs.values())
        
        # Check if all blocks are present
        if self.blocks_df.shape[0] < self.end_height - self.start_init:
            # If not, backfill missing blocks
            set_of_expected_blocks = set(range(self.start_init, self.end_height))
            blocks_present =  set(self.blocks_df['block'].map(lambda x: x['header']['height']).astype(int))
            missing_blocks = set_of_expected_blocks - blocks_present
            print(f"Backfilling {len(missing_blocks)} blocks...")
            for block in missing_blocks:
                self.start_height = block
                self.end_height = block
                self.query_blocks()
            print("Done backfilling blocks.")

        # Check if all transactions are present
        if num_txs_logged == total_txs_from_blocks:
            print("All transactions are present. No need for backfilling.")
        else:
            # If not, backfill missing transactions
            print("Backfilling transactions...")
            txs_heights = set(self.tx_df['height'])
            missing_txs_blocks = {x for x,y in blocks_with_txs.items() if x not in txs_heights}
            print(f'missing txs from {len(missing_txs_blocks)} blocks.')
            self.backfilled_txs = []
            # for block in missing_txs_blocks:
            #     self.start_height = int(block)
            #     self.end_height = int(block) # the query is <=, not <, so don't do +1
            #     self.query_txs(mode='backfill') # currently not done async, but since the URLs are built, you can query all of them async and then paginate as needed

            async with aiohttp.ClientSession() as session:
                print('backfilling async')
                self.backfilled_txs = await self.async_query_txs(missing_txs_blocks, session)

            print(f'{len(self.backfilled_txs)} recovered, but may still be missing data. Look in errors subdirectory.')

            print(f'{len(to_be_added)} txs recovered out of {total_txs_from_blocks - num_txs_logged} total missing txs')
            to_be_added = []
            for data in self.backfilled_txs:
                for page in data:
                    to_be_added.append(page)

            backfilled_tx_df = pd.DataFrame(to_be_added)

            self.tx_df = pd.concat([self.tx_df, backfilled_tx_df]).reset_index(drop=True)
            self.tx_df.to_json(f'./data/{self.network}/{self.protocol}/txs/{self.start_init}_{self.end_init}.json', orient='records')

            print("Done backfilling transactions.")

    def generate_urls(self, endpoint_format: str, total_pages=None):
        """
        Generate the URLs for the RPC API.

        Args:
            endpoint_format (str): A format string for the API endpoint URL.
            total_pages (int): The total number of pages.

        Returns:
            list: A list of URLs.
        """

        if total_pages:
            urls = [endpoint_format.format(api_url=self.api_url, start=self.start_height, end=self.end_height, page=page, per_page=self.per_page)
                for page in range(1, total_pages+1)]

        else:
            remaining_pages_to_iterate = (self.end_height - self.start_height) // self.per_page + 1
            urls = [endpoint_format.format(api_url=self.api_url, start=self.start_height, end=self.end_height, page=page, per_page=self.per_page)
                for page in range(1, remaining_pages_to_iterate+1)]
            
        return urls

    def extract(self):
        """
        Run the extract process.
        """

        start = time.time()
        self.query_blocks()
        self.query_txs()
        self.backfill()

        self.save_json(self.blocks, 'blocks')
        self.save_json(self.tx, 'txs')
        end = time.time()
        print(f"process took {end - start} seconds.")

        print("Done.")

    async def fetch(self, url: str, session):
        """
        Fetch the data from the URL.

        Args:
            url (str): The URL to fetch the data from.
            session (aiohttp.ClientSession): The session to use for the request.

        Returns:
            dict: The JSON data from the response.
        """
        while True:
            try:
                async with session.get(url) as response:
                    if response.status == 429:
                        print(f"Rate limit exceeded. Sleeping for 10 seconds.")
                        await asyncio.sleep(10)  # sleep for 10 seconds
                        continue

                    if response.status != 200:
                        print(f"Failed to get response from {url}, status code: {response.status}")
                        return None

                    if response.content_type != 'application/json':
                        print(f"Unexpected content type in response from {url}, content type: {response.content_type}")
                        return None

                    return await response.json()

            except Exception as e:
                print(f"Failed to get response from {url}")
                print(f"Error details: {e}")
                await asyncio.sleep(5)
                continue


    async def fetch_all(self, urls):
        """
        Fetch the data from all the URLs.

        Args:
            urls (list): The list of URLs to fetch the data from.

        Returns:
            list: The list of JSON data from the responses.
        """
        semaphore = asyncio.Semaphore(self.semaphore)  # limit the number of simultaneous requests
        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in urls:
                task = self.bounded_fetch(semaphore, url, session)
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
            return responses

    async def bounded_fetch(self, semaphore, url, session):
        """
        Fetch the data from the URL with rate limiting.

        Args:
            semaphore (asyncio.Semaphore): The semaphore to limit the number of simultaneous requests.
            url (str): The URL to fetch the data from.
            session (aiohttp.ClientSession): The session to use for the request.

        Returns:
            dict: The JSON data from the response.
        """
        async with semaphore:
            return await self.fetch(url, session)

    async def process_responses(self, responses, data_key):
        """
        Process the responses.

        Args:
            responses (list): The list of responses to process.
            data_key (str): The key in the response JSON where the data is stored.

        Returns:
            list: The list of data from the responses.
        """
        data = []
        for response in responses:
            if response is not None and 'result' in response.keys():
                new_data = response['result'][data_key]
                data.extend(new_data)
        return data


    async def async_extract(self):
        """
        Run the extract process.
        """

        start = time.time()

        block_urls = self.generate_urls(
            endpoint_format='{api_url}/block_search?query="block.height>={start} AND block.height<={end}"&page={page}&per_page={per_page}&order_by="asc"&match_events=true'
        )

        tx_total_count = int(requests.get(f'{self.api_url}/tx_search?query="tx.height>={self.start_init} AND tx.height<={self.end_init}"&page=1&per_page={self.per_page}&order_by="asc"&match_events=true').json()['result']['total_count'])
        tx_total_pages = math.ceil(tx_total_count / self.per_page)
        tx_urls = self.generate_urls(
            endpoint_format='{api_url}/tx_search?query="tx.height>={start} AND tx.height<={end}"&page={page}&per_page={per_page}&order_by="asc"&match_events=true', total_pages=tx_total_pages
        )

        # Fetch the data for each URLsema
        block_responses = await self.fetch_all(block_urls)
        print(f'block_responses complete in {time.time() - start} seconds.')
        tx_responses = await self.fetch_all(tx_urls)
        print(f'tx responses complete in {time.time() - start} seconds.')

        # Process the responses
        self.blocks = await self.process_responses(block_responses, 'blocks')
        print(f'block responses processed in {time.time() - start} seconds.')
        self.txs = await self.process_responses(tx_responses, 'txs')
        print(f'txs responses processed in {time.time() - start} seconds.')

        self.blocks_df = pd.DataFrame(self.blocks)
        self.tx_df = pd.DataFrame(self.txs)
        
        await self.backfill()
        print(f'backfilling complete in {time.time() - start} seconds.')

        # download
        self.save_json(self.blocks, 'blocks')
        self.save_json(self.txs, 'txs')

        end = time.time()
        print(f"process took {end - start} seconds.")

        print("Done.")

def get_min_height(api_url):
    r = requests.get(f'{api_url}/block?height=1')
    json = r.json()

    if 'result' in json.keys():
        return 1
    
    else:
        min_block = int(json['error']['data'].split(' ')[-1])
    return min_block

def get_max_height(api_url):
    r = requests.get(f'{api_url}/abci_info?')
    json = r.json()
    max_block = int(json['result']['response']['last_block_height'])

    return max_block

def get_min_ingested_height(directory):
    files = glob.glob(f"{directory}/*.json")
    min_height = min(int(file.split('/')[-1].split('_')[0]) for file in files) if files else 0
    return min_height

def get_max_ingested_height(directory):
    files = glob.glob(f"{directory}/*.json")
    if files:
        max_height = max(int(file.split('/')[-1].split('_')[1].split('.')[0]) for file in files)
    else:
        max_height = 0  # or some other appropriate value
        
    return max_height


if __name__ == "__main__":
    extractor = DataExtractor(api_url='rpc_url', start_height=10000000, per_page=50, end_height=10100000-1, protocol='rpc', network='akash', semaphore=3)
    # extractor.extract()
    asyncio.run(extractor.extract())