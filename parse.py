
import base64
import glob
import os
import datetime
import orjson
import numpy as np
import pandas as pd
#import modin.pandas as pd
from typing import Tuple

class DataParser:
    def __init__(self, blocks_path:str, txs_path: str, output_path: str):
        """
        Initialize DataParser with paths to the blocks and transactions data.
        
        Args:
            blocks_path (str): Path to the blocks data.
            txs_path (str): Path to the transactions data.
            output_path (str): Path to output the parsed data.
        """

        self.blocks_path = blocks_path
        self.txs_path = txs_path
        self.output_path = output_path
        self.blocks_df = None
        self.txs_df = None
        self.df_log_attributes = None
        self.events_df_wide = None
        self.df_tx_result = None

    @staticmethod
    def safe_orjson_loads(data: str):
        """
        Safely load JSON data. If loading fails, return the original data.
        
        Args:
            data (str): The data to load.
        
        Returns:
            str: The loaded data, or the original data if loading failed.
        """
        try:
            if isinstance(data, str):
                return orjson.loads(data)
            else:
                return data
        except orjson.JSONDecodeError:
            return data

    @staticmethod
    def decode_base64(data: str) -> str:
        """
        Decode base64 data.
        
        Args:
            data (str): The base64 data to decode.
        
        Returns:
            str: The decoded data.
        """
        if data is None:
            return None
        return str(base64.b64decode(data), 'utf-8')

    def get_parsed_files(self):
        # Check if the directory exists and create it if necessary
        directory = os.path.dirname(f'{self.output_path}/parsed_files.json')
        if not os.path.exists(directory):
            os.makedirs(directory)

        try:
            if os.path.getsize(f'{self.output_path}/parsed_files.json') > 0:  # checks if file is not empty
                with open(f'{self.output_path}/parsed_files.json', 'rb') as file:
                    parsed_files = orjson.loads(file.read())
                    if not isinstance(parsed_files, dict):
                        parsed_files = {'blocks': [], 'txs': []}
            else:
                parsed_files = {'blocks': [], 'txs': []}
        except FileNotFoundError:
            parsed_files = {'blocks': [], 'txs': []}

        return parsed_files

    def update_parsed_files(self, new_files, data_type):
         # Check if the directory exists and create it if necessary
        directory = os.path.dirname(f'{self.output_path}/parsed_files.json')
        if not os.path.exists(directory):
            os.makedirs(directory)
            
        parsed_files = self.get_parsed_files()
        parsed_files[data_type].extend(new_files)

        with open(f'{self.output_path}/parsed_files.json', 'w') as file:
            file.write(orjson.dumps(parsed_files).decode('utf-8'))

    def load_new_json(self, directory: str, data_type: str) -> pd.DataFrame:
        parsed_files = self.get_parsed_files()

        json_files = glob.glob(f"{directory}/*.json")
        json_files = [file for file in json_files 
                    if file.split('/')[-1] not in parsed_files]  # Only new files

        dfs = [pd.read_json(file) for file in json_files]
        df = pd.concat(dfs, ignore_index=True)

        if not df.empty:
            new_files = [file.split('/')[-1] for file in json_files]
            self.update_parsed_files(new_files, data_type)

        return df

    @staticmethod
    def load_all_json(directory: str) -> pd.DataFrame:
        """
        Load all JSON files in a directory into a pandas DataFrame.
        
        Args:
            directory (str): The directory where the JSON files are located.
        
        Returns:
            pd.DataFrame: A DataFrame containing all the loaded data.
        """

        json_files = glob.glob(f"{directory}/*.json")
        dfs = [pd.read_json(file) for file in json_files]
        df = pd.concat(dfs, ignore_index=True)
        return df

    def parse_blocks(self) -> None:
        """
        Parse the 'block' information from a DataFrame containing block data.
        """
        self.blocks_df[['height', 'chain_id', 'time', 'proposer_address']] = self.blocks_df['block'].apply(lambda x: pd.Series([x['header']['height'], x['header']['chain_id'], x['header']['time'], x['header']['proposer_address']]))
        self.blocks_df['height'] = self.blocks_df['height'].astype(int)
        self.blocks_df['day'] = pd.to_datetime(self.blocks_df['time']).dt.to_period('D').astype(str)
        self.blocks_df['month'] = pd.to_datetime(self.blocks_df['time']).dt.to_period('M').astype(str)
        self.blocks_df['year'] = pd.to_datetime(self.blocks_df['time']).dt.to_period('Y').astype(str)
        self.blocks_df = self.blocks_df[['height', 'chain_id', 'time', 'proposer_address', 'day', 'month', 'year']]

    def parse_txs(self) -> None:
        """
        Parse the transactions from a DataFrame containing transaction data.
        """
        self.df_tx_result = pd.json_normalize(self.txs_df['tx_result'])
        self.df_tx_result[['hash', 'height']] = self.txs_df[['hash', 'height']]

    def parse_logs(self) -> None:
        """
        Parse the logs from a DataFrame containing transaction results.
        """
        self.df_tx_result['log'] = self.df_tx_result['log'].apply(self.safe_orjson_loads)
        df_logs = self.df_tx_result[['hash', 'height', 'log']].explode('log').reset_index()
        expanded_logs = pd.json_normalize(df_logs['log']).fillna(0)
        df_logs[['events', 'msg_index']] = expanded_logs
        df_log_events = df_logs[['hash', 'height', 'msg_index', 'events']].explode('events').reset_index(drop=True)
        df_log_events_normalized = pd.json_normalize(df_log_events['events'])
        df_log_events[['type', 'attributes']] = df_log_events_normalized
        df_log_attributes = df_log_events[['hash', 'height', 'msg_index', 'type', 'attributes']].explode('attributes').reset_index(drop=True)
        df_log_attributes_normalized = pd.json_normalize(df_log_attributes['attributes'])
        df_log_attributes[['key', 'value']] = df_log_attributes_normalized
        self.df_log_attributes = df_log_attributes[['hash', 'height', 'msg_index', 'type', 'key', 'value']]

    def parse_events_wide(self) -> None:
        """
        Parse the events from the logs and transform the DataFrame into a wide format.
        """
        event_list = [event for events in self.txs_df['tx_result'].apply(lambda x: x['events']) for event in events]
        event_df = pd.DataFrame(event_list)
        attr_df = pd.concat([pd.DataFrame(x) for x in event_df['attributes']], ignore_index=True)
        attr_df['key'] = attr_df['key'].apply(self.decode_base64)
        attr_df['value'] = attr_df['value'].apply(self.decode_base64)
        event_df = event_df.drop('attributes', axis=1).join(attr_df)
        event_df['hash'] = np.repeat(self.txs_df['hash'].values, self.txs_df['tx_result'].apply(lambda x: len(x['events'])))
        event_df['height'] = np.repeat(self.txs_df['height'].values, self.txs_df['tx_result'].apply(lambda x: len(x['events'])))
        event_df.reset_index(drop=True, inplace=True)
        event_df['combined_key'] = event_df['type'] + '_' + event_df['key']
        event_df['occurrence'] = event_df.groupby(['hash', 'height', 'combined_key']).cumcount()
        self.events_df_wide = event_df.pivot(index=['hash', 'height', 'occurrence'], columns='combined_key', values='value')
        self.events_df_wide.reset_index(inplace=True)

    def save_as_partitioned_parquet(self, df: pd.DataFrame, name: str) -> None:
        """
        This function saves a DataFrame as a partitioned Parquet file.
        
        Args:
            df (pd.DataFrame): The DataFrame to save.
            name (str): The name of the table (used for creating a directory).

        Returns:
            None
        """
        # Ensure the output directory exists"""
        os.makedirs(self.output_path, exist_ok=True)

        # Create a new directory for the table
        table_dir = os.path.join(self.output_path, name)
        os.makedirs(table_dir, exist_ok=True)

        df.to_parquet(table_dir, engine='pyarrow', partition_cols=['year', 'month', 'day'], index=False)

    def run(self):
        """
        Run the DataParser.
        
        This method loads the blocks and transactions data, parses them, and saves the parsed data as partitioned Parquet files.
        """
        #self.blocks_df = self.load_all_json(self.blocks_path)
        self.blocks_df = self.load_new_json(self.blocks_path, 'blocks')
        self.parse_blocks()
        self.save_as_partitioned_parquet(df=self.blocks_df, name='blocks')

        self.txs_df = self.load_new_json(self.txs_path, 'txs')
        self.parse_txs()
        self.parse_logs()
        self.parse_events_wide()

        # Join the 'time' column from blocks_df into df_log_attributes and events_df_wide
        self.df_tx_result = self.df_tx_result.merge(self.blocks_df[['height', 'time', 'day', 'month', 'year']], on=['height'])
        self.df_log_attributes = self.df_log_attributes.merge(self.blocks_df[['height', 'time', 'day', 'month', 'year']], on=['height'])
        self.events_df_wide = self.events_df_wide.merge(self.blocks_df[['height', 'time', 'day', 'month', 'year']], on=['height'])

        # Save dataframes as partitioned parquet files
        self.save_as_partitioned_parquet(df=self.df_tx_result[['hash', 'height', 'time', 'day', 'month', 'year', 'gas_wanted', 'gas_used', 'code', 'codespace', 'info']], name='tx_result')
        self.save_as_partitioned_parquet(df=self.df_log_attributes, name='log_attributes')
        self.save_as_partitioned_parquet(df=self.events_df_wide, name='events')

if __name__ == "__main__":
    parser = DataParser(blocks_path='path/to/blocks', txs_path='path/to/txs', output_path='path/to/output')
    parser.run()