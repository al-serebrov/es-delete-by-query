# Purpose

For newer versions of ElasticSearch (> 5.0) there's such functionality "out of the box", check the official documentation [here](https://www.elastic.co/guide/en/elasticsearch/reference/5.0/docs-delete-by-query.html)

But for older versions (< 5.0), for example for 2.4, you can use this simple python script to perform delete by query.

## Installation
The script depends only on official [ElasticSearch python client](https://elasticsearch-py.readthedocs.io/en/master/)

Make the following steps to install needed requirements into isolated virtual environment:
```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

Script takes settings via command line arguments:

```
$ python delete_by_query.py --help
usage: delete_by_query.py [-h] [-i INDEX] [-t DOC_TYPE]

Delete documents from ElasticSearch by query.

optional arguments:
  -h, --help            show this help message and exit
  -i INDEX, --index INDEX
                        Index name, defaults to index
  -t DOC_TYPE, --type DOC_TYPE
                        Document type, defaults to document
```

To change the query used to find (and then delete) documents you need to go ahead and change the source code.
