from delete_by_query import delete_by_query
from elasticsearch import Elasticsearch
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Delete documents from ElasticSearch by query.'
    )

    parser.add_argument(
        '-u',
        '--url',
        action='store',
        dest='es_url',
        default='http://localhost:9200',
        help='Elasticsearch URL, defaults to http://localhost:9200',
        required=False
    )

    parser.add_argument(
        '-i',
        '--index',
        action='store',
        dest='index',
        default='index',
        help='Index name, defaults to index',
        required=False
    )
    parser.add_argument(
        '-t',
        '--type',
        action='store',
        dest='doc_type',
        default='document',
        help='Document type, defaults to document',
        required=False
    )

    parser.add_argument(
        '-f',
        '--file',
        action='store',
        dest='query_filename',
        default=None,
        help='The file with ES query to find and delete documents',
        required=False
    )

    results = parser.parse_args()
    es_url = results.es_url
    index = results.index
    doc_type = results.doc_type
    query_filename = results.query_filename

    if query_filename:
        delete_query = open(query_filename, 'r').read()
    else:
        delete_query = {
          "query": {
            "bool": {
              "must": [
                {
                  "exists": {
                    "field": "categories.cat_3"
                  }
                }
              ]
            }
          }
        }
    # Setup elasticsearch connection.
    es = Elasticsearch(
        [es_url],
        # sniff before doing anything
        sniff_on_start=True,
        # refresh nodes after a node fails to respond
        sniff_on_connection_fail=True,
        # and also every 60 seconds
        sniffer_timeout=60
    )
    delete_by_query(
        es=es,
        index=index,
        doc_type=doc_type,
        query=delete_query
    )
