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
ES_DOC_ID_MAX_SIZE = 512


def set_data(file_entry, send_time, fields_obj):
    """ Generator function used with bulk helper API """
    assert isinstance(file_entry, paths.QuantumEntry)
    
    with open(file_entry.abspath, "rb") as log_file:
        try:
            for line_num,line in enumerate(log_file):
                
                # New Doc ID is the file's path + / + line number starting at 1
                new_doc_id = (file_entry/str(line_num+1)).relpath
                if len(new_doc_id) >= ES_DOC_ID_MAX_SIZE:
                    # Store hash if path exceeds ES limit
                    new_doc_id = hash(new_doc_id)
                
                yield {
                    '_id': new_doc_id,
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
            logging.warning("Error reading %s. Non utf-8 encoding?", file_entry.abspath)
            return


def send_to_es(es_obj, fields_obj, file_entry):
    """
    Sends the contents of the given file to ES with the attached
    fields. The system time of the call is also attached and sent
    es_obj:
        Elasticsearch object
    fields_obj: 
        object containing all the fields
    file_entry:
        file that is being sent to Elasticsearch
    """
    #Epoch milliseconds
    send_time = int(round(time.time() * 1000))  

    try:
        error = False
        logging.debug("Indexing: %s", file_entry.relpath)
        data = set_data(file_entry, send_time, fields_obj)
        for success,info in helpers.parallel_bulk(es_obj,data,index=INDEX_NAME,doc_type='_doc'):
            if not success:
                error = True
        
        if error:
            logging.critical("Unable to index: %s", file_entry.abspath)
            return False
        else:
            logging.debug("Indexed: %s", file_entry.relpath)
            return True

    except elasticsearch.exceptions.ConnectionError:
        logging.critical("Connection error sending doc %s to elastic search", file_path)
        return False
    
    except UnicodeDecodeError:
        logging.warning("Error reading %s. Non utf-8 encoding?", file_entry.abspath)
        return False

