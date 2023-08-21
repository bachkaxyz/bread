import pytest
from unittest.mock import patch, Mock, MagicMock
import extract
from extract import DataExtractor, get_min_height, get_max_height, get_min_ingested_height, get_max_ingested_height
import asyncio
import json
from unittest.mock import mock_open

MOCK_METADATA = """
{
    "min_height": 100,
    "max_height": 300
}
"""

# Mock data
MOCK_FILE_LIST = ['dummy_directory/100_200.json', 'dummy_directory/200_300.json']

# Mocking the API response for get_min_height and get_max_height
MOCK_MIN_HEIGHT_RESPONSE = {"result": {"some_key": "some_value"}}
MOCK_MAX_HEIGHT_RESPONSE = {"result": {"response": {"last_block_height": "10000"}}}

MOCK_REQUESTS_GET_RESPONSE = Mock()
MOCK_REQUESTS_GET_RESPONSE.json.return_value = {
    'result': {
        'total_count': '100'
    }
}

@patch("extract.get_max_ingested_height")
@patch("extract.get_min_ingested_height")
@patch("extract.get_max_height_from_files")
@patch("extract.get_min_height_from_files")
def test_validate_metadata_matching(mock_min_files, mock_max_files, mock_min_ingested, mock_max_ingested):
    mock_min_ingested.return_value = 100
    mock_max_ingested.return_value = 300
    mock_min_files.return_value = 100
    mock_max_files.return_value = 300

    assert extract.validate_metadata("dummy_directory") == True

@patch("extract.get_max_ingested_height")
@patch("extract.get_min_ingested_height")
@patch("extract.get_max_height_from_files")
@patch("extract.get_min_height_from_files")
def test_validate_metadata_non_matching(mock_min_files, mock_max_files, mock_min_ingested, mock_max_ingested):
    mock_min_ingested.return_value = 100
    mock_max_ingested.return_value = 250  # This doesn't match with file max
    mock_min_files.return_value = 100
    mock_max_files.return_value = 300

    assert extract.validate_metadata("dummy_directory") == False

@patch("extract.get_min_height_from_files", return_value=100)
@patch("extract.get_max_height_from_files", return_value=300)
@patch("extract.write_metadata")
def test_update_metadata_from_files(mock_write_metadata, _, __):
    extract.update_metadata_from_files("dummy_directory")
    mock_write_metadata.assert_called_once_with("dummy_directory", 100, 300)

@patch("extract.glob.glob", return_value=MOCK_FILE_LIST)
def test_get_min_height_from_files(_):
    assert extract.get_min_height_from_files("dummy_directory") == 100

@patch("extract.glob.glob", return_value=MOCK_FILE_LIST)
def test_get_max_height_from_files(_):
    assert extract.get_max_height_from_files("dummy_directory") == 300

@patch("requests.get")
def test_get_min_height(mock_get):
    mock_get.return_value.json.return_value = MOCK_MIN_HEIGHT_RESPONSE
    assert get_min_height("dummy_url") == 1


@patch("requests.get")
def test_get_max_height(mock_get):
    mock_get.return_value.json.return_value = MOCK_MAX_HEIGHT_RESPONSE
    assert get_max_height("dummy_url") == 10000

@pytest.mark.parametrize("file_content, expected", [
    (MOCK_METADATA, 100),
    (None, 0)  # Simulating a FileNotFoundError scenario
])
@patch("builtins.open", new_callable=mock_open, read_data=MOCK_METADATA)
@patch("orjson.loads", return_value=json.loads(MOCK_METADATA))
def test_get_min_ingested_height(mock_orjson_load, mock_open_func, file_content, expected):
    if file_content is None:
        mock_open_func.side_effect = FileNotFoundError()
    assert get_min_ingested_height("dummy_directory") == expected

@pytest.mark.parametrize("file_content, expected", [
    (MOCK_METADATA, 300),
    (None, 0)  # Simulating a FileNotFoundError scenario
])
@patch("builtins.open", new_callable=mock_open, read_data=MOCK_METADATA)
@patch("orjson.loads", return_value=json.loads(MOCK_METADATA))
def test_get_max_ingested_height(mock_orjson_load, mock_open_func, file_content, expected):
    if file_content is None:
        mock_open_func.side_effect = FileNotFoundError()
    assert get_max_ingested_height("dummy_directory") == expected

@patch.object(DataExtractor, "fetch_all", return_value=[])
@patch.object(DataExtractor, "process_responses", return_value=[])
@patch.object(DataExtractor, "save_json")
@patch("requests.get", return_value=MOCK_REQUESTS_GET_RESPONSE)
def test_data_extractor_async_extract(mock_requests_get, mock_save_json, mock_process_responses, mock_fetch_all):
    extractor = DataExtractor(api_url='https://dummy_url', start_height=100, end_height=200, per_page=50, protocol='rpc', network='akash', semaphore=3)
    # Given the mocks, this shouldn't raise any exceptions
    pytest.mark.asyncio
    async def run_test():
        await extractor.async_extract()
    run_test()