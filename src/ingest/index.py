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

import paths


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
                        'major_version': fields_obj.sg_ver[0],
                        'minor_version': fields_obj.sg_ver[1],
                        'platform': fields_obj.platform,
                        'categorize_time': send_time,
                        'message': line.decode('utf-8')
                    }
                }
        except UnicodeDecodeError:
            # Only supporting utf-8 for now. Skip others.
            logging.warning("Error reading %s. Non utf-8 encoding?", file_path)
            return


def send_to_es(es_obj, fields_obj, file_entry):
    """
    Sends the contents of the given file to ES with the attached
    fields. The system time of the call is also attached and sent.
    """
    assert isinstance(file_entry, paths.QuantumEntry)
    
    send_time = int(round(time.time() * 1000))  # Epoch milliseconds
    
    try:
        error = False
        logging.debug("Indexing: %s", file_path)
        for success, info in helpers.parallel_bulk(es_obj, set_data(file_path, send_time, fields_obj), index=INDEX_NAME, doc_type='_doc'):
            if not success:
                error = True
        
        if error:
            logging.critical("Unable to index: %s", file_path)
            return False
        else:
            logging.debug("Indexed: %s", file_path)
            return True

    except elasticsearch.exceptions.ConnectionError:
        logging.critical("Connection error sending doc %s to elastic search (file too big?)", file_path)
        return False
    
    except UnicodeDecodeError:
        logging.warning("Error reading %s. Non utf-8 encoding?", file_path)
        return False

