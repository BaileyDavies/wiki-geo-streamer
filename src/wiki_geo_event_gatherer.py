import asyncio
import websockets
import json
import jsonschema
from jsonschema import validate
from socket import gaierror
from src.elasticsearch_helper import AWSElasticsearchClient
from datetime import datetime

WIKI_STREAM_WEBSOCKET_ADDRESS = "ws://wikimon.hatnote.com:9000"

# Defines the fields and their types that we expect in the stream results
WIKI_STREAM_JSON_EXPECTED = {
    "type": "object",
    "properties": {
        "action": {"enum": ["edit", "new"]},
        "geo_ip": {
            "properties": {
                "city": {"type": "string"},
                "country_name": {"type": "string"},
                "latitude": {"type": "number"},
                "longitude": {"type": "number"},
            },
            "required": ["city", "country_name"]
        },
        "change_size": {"type": "number"},
        "flags": {"type": ["string", "array", "null"]},
        "hashtags": {"type": ["array", "null"]},
        "is_anon": {"type": "boolean"},
        "is_bot": {"type": "boolean"},
        "is_minor": {"type": "boolean"},
        "is_new": {"type": "boolean"},
        "is_unpatrolled": {"type": "boolean"},
        "mentions": {"type": ["array", "null"]},
        "ns": {"type": "string"},
        "page_title": {"type": "string"},
        "parent_rev_id": {"type": "string"},
        "parsed_summary": {"type": ["string", "null"]},
        "rev_id": {"type": "string"},
        "section": {"type": "string"},
        "summary": {"type": ["string", "null"]},
        "url": {"type": "string"},
        "user": {"type": "string"},
    },
    "required": ["geo_ip", "change_size", "page_title"]
}


def upload_valid_wiki_results_to_elasticsearch(wiki_results, client):
    es_results = []
    for wiki_result in wiki_results:
        # We need to combine the longitude and latitude values to create a geo-point
        wiki_result["location"] = '{},{}'.format(wiki_result["geo_ip"]["latitude"], wiki_result["geo_ip"]["longitude"])
        wiki_result["datetime"] = datetime.now()
        es_res = client.add_to_index("geo_index", wiki_result)
        if es_res["result"] == "created":
            es_results.append(es_res)
        else:
            print('An error has occurred in the ES upload process: {}'.format(es_res))
    return es_results


def validate_wiki_json_object(wiki_event_object):
    # Try and validate what we get from the websocket stream against our defined schema
    try:
        validate(instance=wiki_event_object, schema=WIKI_STREAM_JSON_EXPECTED)
    except jsonschema.exceptions.ValidationError:
        return False
    return True


async def connect_to_websocket_and_get_valid_events(websocket_address, num_of_events_target):
    valid_events = []
    try:
        async with websockets.connect(websocket_address) as ws:
            num_of_events = 0
            while num_of_events < num_of_events_target:
                wiki_event = await ws.recv()
                wiki_event_object = json.loads(wiki_event)
                wiki_event_is_valid = validate_wiki_json_object(wiki_event_object)
                if wiki_event_is_valid:
                    num_of_events = num_of_events + 1
                    valid_events.append(wiki_event_object)
            return valid_events
    except gaierror as e:
        return "Could not connect with error: {}".format(e)


def run_wiki_websocket_upload_results_to_elastic(num_of_geo_results_to_gather):
    valid_wiki_results = asyncio.get_event_loop().run_until_complete(connect_to_websocket_and_get_valid_events(
        WIKI_STREAM_WEBSOCKET_ADDRESS, num_of_geo_results_to_gather))
    if len(valid_wiki_results) > 0:
        es_client = AWSElasticsearchClient()
        upload_valid_wiki_results_to_elasticsearch(valid_wiki_results, es_client)
