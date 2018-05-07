# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch, helpers, exceptions
from pprint import pprint
import argparse


def delete_docs(es_url, index, doc_type, query):

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

    # Start the initial search.
    page = es.search(
        index=index,
        doc_type=doc_type,
        scroll='2m',
        search_type='scan',
        body=query
    )
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    # Start scrolling
    try:
        while (scroll_size > 0):
            print("Scrolling...")
            page = es.scroll(scroll_id=sid, scroll='2m')
            # Update the scroll ID
            sid = page['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scroll_size = len(page['hits']['hits'])
            print("scroll size: " + str(scroll_size))
            # Delete these results
            bulk_body = []
            for rec in page['hits']['hits']:
                bulk_body.append({
                    "_op_type": "delete",
                    "_index": index,
                    "_type": doc_type,
                    "_id": rec['_id']
                })
            pprint(bulk_body)
            print('Deleting records')
            helpers.bulk(es, bulk_body)
            print('Delete successful')

    except exceptions.NotFoundError:
        pass
    except exceptions.TransportError as ex:
        print("Elasticsearch error: " + ex.error)
        raise ex


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Delete documents from ElasticSearch by query.'
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

    results = parser.parse_args()
    index = results.index
    doc_type = results.doc_type

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
    # Provide a proper ES url, would be like 172.18.0.2:9200
    delete_docs(
        es_url='http://localhost:9200',
        index=index,
        doc_type=doc_type,
        query=delete_query
    )
