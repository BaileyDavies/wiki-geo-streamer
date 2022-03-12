import unittest
import asyncio
from src.elasticsearch_helper import AWSElasticsearchClient
from src.wiki_geo_event_gatherer import connect_to_websocket_and_get_valid_events
from src.wiki_geo_event_gatherer import upload_valid_wiki_results_to_elasticsearch

WIKI_STREAM_WEBSOCKET_ADDRESS = "ws://wikimon.hatnote.com:9000"


# Integration Tests
class TestWikiGeoValidatorIntegrations(unittest.TestCase):
    # It should successfully get 3 verified results from the websocket
    def test_get_data_from_websocket_and_validate(self):
        # It should get 3 valid results from the websockets
        wiki_results = asyncio.get_event_loop().run_until_complete(connect_to_websocket_and_get_valid_events(
            WIKI_STREAM_WEBSOCKET_ADDRESS, 3))
        self.assertTrue(len(wiki_results), 3)

    def test_get_data_from_wrong_address(self):
        # It should throw an error when it can't connect to a websocket
        res = asyncio.get_event_loop().run_until_complete(connect_to_websocket_and_get_valid_events(
            "ws://wikimon.hatnte.com:9000", 3))
        self.assertEqual("Could not connect with error: [Errno 11001] getaddrinfo failed", res)

    def test_get_valid_data_upload_successfully_to_elk(self):
        es_client = AWSElasticsearchClient()
        # It should get one valid result and upload it to the elasticsearch endpoint successfully
        valid_wiki_results = asyncio.get_event_loop().run_until_complete(connect_to_websocket_and_get_valid_events(
            WIKI_STREAM_WEBSOCKET_ADDRESS, 1))
        es_result = upload_valid_wiki_results_to_elasticsearch(valid_wiki_results, es_client)
        self.assertEqual(len(es_result), 1)
