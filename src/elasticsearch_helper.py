import boto3
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import os
import uuid


class AWSElasticsearchClient:
    def __init__(self):
        self.host = os.environ.get('ES_HOST')
        self._region = 'eu-central-1'
        self._service = 'es'
        self._credentials = boto3.Session(aws_access_key_id=os.environ.get('ES_KEY'),
                                          aws_secret_access_key=os.environ.get('ES_SEC_KEY')).get_credentials()
        self._aws_auth = AWS4Auth(self._credentials.access_key, self._credentials.secret_key, self._region,
                                  self._service,
                                  session_token=self._credentials.token)
        self.es_client = self.get_elasticsearch_client()

    def get_elasticsearch_client(self):
        # Build the Elasticsearch client.
        es = Elasticsearch(
            hosts=[{'host': self.host, 'port': 443}],
            http_auth=self._aws_auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        return es

    def add_to_index(self, index_name, data):
        return self.es_client.index(index=index_name, id=uuid.uuid4(), body=data)
