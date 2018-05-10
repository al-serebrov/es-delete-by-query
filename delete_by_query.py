# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch, helpers, exceptions
import argparse
import logging

logger = logging.getLogger('update_validated')
logger.setLevel(level=logging.DEBUG)

# create console handler and set level to info
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create file handler and set level to ERROR
fh = logging.FileHandler('log.txt')
fh.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# add formatter to console and file handlers
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# add ch and fh to logger
logger.addHandler(ch)
logger.addHandler(fh)


def delete_docs(es_url, index, doc_type, query):
    """Delete documents from ES index by query.

    Params:
        es_url - ElasticSearch url, for example 'http://localhost:9200'
            if you're running ElasticSearch locally, or the name of the Docker
            container, discoverable by Docker Discovery Service,
            like 'http://es:9200'
        index - the name of index in which documents will be deleted
        doc_type - document type
        query - Lucene ElasticSearch query

    Raises:
        NotFoundError
        TransportError
    """
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
            logger.info("Scrolling...")
            page = es.scroll(scroll_id=sid, scroll='2m')
            # Update the scroll ID
            sid = page['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scroll_size = len(page['hits']['hits'])
            logger.info("scroll size: " + str(scroll_size))
            # Delete these results
            bulk_body = []
            for rec in page['hits']['hits']:
                bulk_body.append({
                    "_op_type": "delete",
                    "_index": index,
                    "_type": doc_type,
                    "_id": rec['_id']
                })
            logger.info('Deleting records')
            helpers.bulk(es, bulk_body)
            logger.info('Delete successful')

    except exceptions.NotFoundError as ex:
        logger.error("Elasticsearch error: " + ex.error)
        raise ex
    except exceptions.TransportError as ex:
        logger.error("Elasticsearch error: " + ex.error)
        raise ex


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
    # Provide a proper ES url, would be like 172.18.0.2:9200
    delete_docs(
        es_url=es_url,
        index=index,
        doc_type=doc_type,
        query=delete_query
    )
