# -*- coding: utf-8 -*-
import logging
from elasticsearch import helpers, exceptions


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


def delete_by_query(es, index, doc_type, query):
    """Delete documents from ES index by query.

    Params:
        es - ElasticSearch connection, for example
            es = Elasticsearch(
                [es_url],
                # sniff before doing anything
                sniff_on_start=True,
                # refresh nodes after a node fails to respond
                sniff_on_connection_fail=True,
                # and also every 60 seconds
                sniffer_timeout=60
            )
        index - the name of index in which documents will be deleted
        doc_type - document type
        query - Lucene ElasticSearch query

    Raises:
        NotFoundError
        TransportError
    """

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
