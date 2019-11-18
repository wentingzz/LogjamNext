"""
@author Wenting Zheng
@author Nathaniel Brooks

This file is for indexing files to Elasticsearch
"""


import os
import time
import shutil
import logging

import elasticsearch
from elasticsearch import Elasticsearch, helpers

import fields


INDEX_NAME = "logjam"


def set_data(file_path, send_time, fields_obj):
    """ Generator function used with bulk helper API """
    with open(file_path, "rb") as log_file:
        try:
            for line in log_file:
                yield {
                    '_source': {
                        'case': fields_obj.case_num,
                        'node_name': fields_obj.node_name,
                        'storagegrid_version': fields_obj.sg_ver,
                        'platform': fields_obj.platform,
                        'categorize_time': send_time,
                        'message': line.decode('utf-8')
                    }
                }
        except UnicodeDecodeError:
            # Only supporting utf-8 for now. Skip others.
            logging.warning("Error reading %s. Non utf-8 encoding?", file_path)
            return


def send_to_es(es_obj, fields_obj, file_path):
    """
    Sends the contents of the given file to ES with the attached
    fields. The system time of the call is also attached and sent.
    """
    send_time = int(round(time.time() * 1000))  # Epoch milliseconds
    
    if not es_obj:                              # no ES connection ready, just return
        return
    
    try:
        error = False
        logging.debug("Indexing %s to Elasticsearch", file_path)
        for success, info in helpers.parallel_bulk(es_obj, set_data(file_path, send_time, fields_obj), index=INDEX_NAME, doc_type='_doc'):
            if not success:
                error = True
        if error:
            logging.critical("Unable to index %s to Elasticsearch", file_path)
        else:
            logging.debug("Indexed %s to Elasticsearch", file_path)

    except elasticsearch.exceptions.ConnectionError:
        logging.critical("Connection error sending doc %s to elastic search (file too big?)", file_path)
    
    except UnicodeDecodeError:
        logging.warning("Error reading %s. Non utf-8 encoding?", file_path)
    
    return

