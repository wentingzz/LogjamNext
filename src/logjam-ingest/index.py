"""
@author Wenting Zheng

This file is to process, and index files to Elasticsearch
"""


import os
import time
import shutil
import logging

import elasticsearch
from elasticsearch import Elasticsearch, helpers

import fields


INDEX_NAME = "logjam"

# Valid extensions to ingest
validExtensions = [".txt", ".log"]
# Valid extentionless files used in special cases
validFiles = ["syslog", "messages", "system_commands"]


def stash_node_in_elk(fullPath, caseNum, es = None):
    """ Stashes a node in ELK stack;
    fullPath : string
        absolute path of the node
    caseNum : string
        StorageGRID case number for this file
    es: Elasticsearch
        Elasticsearch instance
    """
    assert caseNum != None, "Null reference"
    assert caseNum != "0", "Not a valid case number: "+caseNum
    
    files = process_files_in_node(fullPath, [])
    nodefields = fields.extract_fields(fullPath, inherit_from=fields.NodeFields(case_num=caseNum))
    if es:
        for file in files:
            send_to_es(es, nodefields, file)
    return


def set_data(file_path, send_time, fields_obj):
    """ Generator function used with bulk helper API """
    with open(file_path) as log_file:
        try:
            for line in log_file:
                yield {
                    '_source': {
                        'case': fields_obj.case_num,
                        'node_name': fields_obj.node_name,
                        'storagegrid_version': fields_obj.sg_ver,
                        'platform': fields_obj.platform,
                        'categorize_time': send_time,
                        'message': line
                    }
                }
        except UnicodeDecodeError:
            # Only supporting utf-8 for now. Skip others.
            logging.warning("Error reading %s. Non utf-8 encoding?", file_path)
            return


def process_files_in_node(src, file_list):
    """ Finds all the files in the node; returns all the content as a array
    src : string
        absolute path of the node
    file_list: array of string
        array of the content. Each element is the content of a file in the node
    """
    for fileOrDir in os.listdir(src):
        fullFileOrDirPath = os.path.join(src, fileOrDir)
        filename, extension = os.path.splitext(fileOrDir)
        if os.path.isfile(fullFileOrDirPath) and (extension in validExtensions or filename in validFiles) and is_storagegrid(fullFileOrDirPath):
            file_list.append(fullFileOrDirPath)
        elif os.path.isdir(fullFileOrDirPath):
            process_files_in_node(fullFileOrDirPath, file_list)
    return file_list


def stash_file_in_elk(fullPath, filenameAndExtension, caseNum, es = None):
    """ Stashes file in ELK stack; checks if duplicate, computes important
    fields like log category, and prepares for ingest by Logstash.
    fullPath : string
        absolute path of the file
    filenameAndExtension : string
        filename + extension of the file, precomputed before function call
    caseNum : string
        StorageGRID case number for this file
    es: Elasticsearch
        Elasticsearch instance
    """
    assert os.path.isfile(fullPath), "This is not a file: "+fullPath
    assert os.path.splitext(filenameAndExtension)[1] in validExtensions or os.path.splitext(filenameAndExtension)[0] in validFiles, "Not a valid file: "+filenameAndExtension

    assert caseNum != None, "Null reference"
    assert caseNum != "0", "Not a valid case number: "+caseNum

    nodefields = fields.NodeFields(case_num=caseNum)            # only case for fields
    if es:
        send_to_es(es, nodefields, fullPath)                    # send as unknown node
    return


def send_to_es(es_obj, fields_obj, file_path):
    """
    Sends the contents of the given file to ES with the attached
    fields. The system time of the call is also attached and sent.
    """
    time_of_send = int(round(time.time() * 1000))  # Epoch milliseconds
    
    try:
        success, _ = helpers.bulk(
            es_obj,
            set_data(file_path, send_time, fields_obj),
            index=INDEX_NAME,
            doc_type='_doc')
        logging.debug("Indexed %s to Elasticsearch", fullPath)
    
    except elasticsearch.exceptions.ConnectionError:
        logging.critical("Connection error sending doc %s to elastic search (file too big?)", fullPath)
    
    except UnicodeDecodeError:
        logging.warning("Error reading %s. Non utf-8 encoding?", file)
    
    return


def is_storagegrid(full_path):
    """
    Check if a file is StorageGRID file
    """
    if "bycast" in full_path:
        return True

    try:
        with open(full_path) as searchfile:
            for line in searchfile:
                if "bycast" in line:
                    return True
    except Exception as e:
        logging.warning('Error during "bycast" search: %s', str(e))
    
    return False

