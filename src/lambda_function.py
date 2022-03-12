import wiki_geo_event_gatherer
import os


def lambda_handler(event, context):
    # In the future we can hook up this service to an api in order to customise the functionality. i.e number
    # of events to gather. In this case I am just customising this amount with an env var through the service itself
    wiki_geo_event_gatherer.run_wiki_websocket_upload_results_to_elastic(int(os.environ.get('GEO_EVENTS_TO_GATHER')))
