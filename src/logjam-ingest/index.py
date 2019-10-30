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


# Valid extensions to ingest
validExtensions = [".txt", ".log"]
# Valid extentionless files used in special cases
validFiles = ["syslog", "messages", "system_commands"]


def stash_node_in_elk(fullPath, caseNum, categDirRoot, is_owned, es = None):
    """ Stashes a node in ELK stack; 
    fullPath : string
        absolute path of the node
    caseNum : string
        StorageGRID case number for this file
    categDirRoot : string
        directory to stash the file into for Logstash
    is_owned : boolean
        indicates whether the Logjam system owns this file (i.e. can move/delete it)
    es: Elasticsearch
        Elasticsearch instance
    """
    assert caseNum != None, "Null reference"
    assert caseNum != "0", "Not a valid case number: "+caseNum
    timespan = os.path.basename(fullPath)
    nodeName = os.path.basename(os.path.dirname(fullPath))
    gridId = os.path.basename(os.path.dirname(os.path.dirname(fullPath)))
    storageGridVersion = get_storage_grid_version(os.path.join(fullPath, 'system_commands'))
    #TODO platform type
    platform = get_platform(None)
    if not os.path.exists(categDirRoot):
        os.makedirs(categDirRoot)
    timestamp = "%.20f" % time.time()
    basename = "-".join([caseNum, nodeName, timespan, storageGridVersion, timestamp])
    node_dir = os.path.join(categDirRoot, basename)
    if not os.path.exists(node_dir):
        os.makedirs(node_dir)
    files = process_files_in_node(fullPath, node_dir, is_owned, [])
    
    fields = {
        'case': caseNum,
        'node_name': nodeName,
        #'category': category,
        'storagegrid_version': storageGridVersion, 
        'message': files,
        'platform':platform,
        'categorize_time': timestamp
    }
    if es:
        try:
            es.index(index='logjam', doc_type='_doc', body = fields, id=fullPath)
            logging.debug("Indexed %s to Elasticsearch", fullPath)
        except elasticsearch.exceptions.ConnectionError:
            logging.warn("Connection error sending doc %s to elastic search (file too big?)", fullPath)
    
#     helpers.bulk(es, actions)

def process_files_in_node(src, des, is_owned, file_list):
    """ Finds all the files in the node; returns all the content as a array
    src : string
        absolute path of the node
    des : string
        will remove. absolute path of the logjam_categories folder
    is_owned : boolean
        indicates whether the Logjam system owns this file (i.e. can move/delete it)
    file_list: array of string
        array of the content. Each element is the content of a file in the node
    """
    for fileOrDir in os.listdir(src):
        fullFileOrDirPath = os.path.join(src, fileOrDir)
        filename, extension = os.path.splitext(fileOrDir)
        if os.path.isfile(fullFileOrDirPath) and (extension in validExtensions or filename in validFiles) and is_storagegrid(fullFileOrDirPath):
            # TODO: delete move/copy2
            if is_owned:
                try:
                    shutil.move(fullFileOrDirPath, os.path.join(des, filename))     # mv scratch space -> categ folder
                except (IOError) as e:
                    logging.critical("Unable to move file: %s", e)
                    raise e
            else:
                try:
                    shutil.copy2(fullFileOrDirPath, os.path.join(des, filename))    # cp input dir -> categ folder
                except (IOError) as e:
                    logging.critical("Unable to copy file: %s", e)
                    raise e
            with open(fullFileOrDirPath, 'rb') as fp:
                data = fp.read()
                data = data.decode('utf-8', errors='ignore')
                file_list.append(data)
        elif os.path.isdir(fullFileOrDirPath):
            process_files_in_node(fullFileOrDirPath, des, is_owned, file_list)
    return file_list


def stash_file_in_elk(fullPath, filenameAndExtension, caseNum, categDirRoot, is_owned, es = None):
    """ Stashes file in ELK stack; checks if duplicate, computes important
    fields like log category, and prepares for ingest by Logstash.
    fullPath : string
        absolute path of the file
    filenameAndExtension : string
        filename + extension of the file, precomputed before function call
    caseNum : string
        StorageGRID case number for this file
    categDirRoot : string
        directory to stash the file into for Logstash
    is_owned : boolean
        indicates whether the Logjam system owns this file (i.e. can move/delete it)
    es: Elasticsearch
        Elasticsearch instance
    """

    assert os.path.isfile(fullPath), "This is not a file: "+fullPath
    assert os.path.splitext(filenameAndExtension)[1] in validExtensions or os.path.splitext(filenameAndExtension)[0] in validFiles, "Not a valid file: "+filenameAndExtension

    # Log in the database and copy to the appropriate logjam category
    assert caseNum != None, "Null reference"
    assert caseNum != "0", "Not a valid case number: "+caseNum

    files = []
    with open(fullPath, 'rb') as fp:
        data = fp.read()
        data = data.decode('utf-8', errors='ignore')
        files.append(data)

    if not os.path.exists(categDirRoot):
        os.makedirs(categDirRoot)
    
    case_dir = os.path.join(categDirRoot, caseNum)
    if not os.path.exists(case_dir):
        os.makedirs(case_dir)
    
    categDirPath = os.path.join(categDirRoot, caseNum, filenameAndExtension)

    if is_owned:
        try:
            shutil.move(fullPath, categDirPath)     # mv scratch space -> categ folder
        except (IOError) as e:
            logging.critical("Unable to move file: %s", e)
            raise e
    else:
        try:
            shutil.copy2(fullPath, categDirPath)    # cp input dir -> categ folder
        except (IOError) as e:
            logging.critical("Unable to copy file: %s", e)
            raise e

    timestamp = "%.20f" % time.time()
    basename = "-".join([filenameAndExtension, timestamp])
    categDirPathWithTimestamp = os.path.join(categDirRoot, caseNum, basename)

    fields = {
        'case': caseNum,
        'node_name': 'unknown',
        #'category': category,
        'storagegrid_version': 'unknown', 
        'message': files,
        'platform':'unknown',
        'categorize_time': timestamp
    }
    if es:
        try:
            es.index(index='logjam', doc_type='_doc', body = fields, id=fullPath)
            logging.debug("Indexed %s to Elasticsearch", fullPath)
        except elasticsearch.exceptions.ConnectionError:
            logging.warn("Connection error sending doc %s to elastic search (file too big?)", fullPath)
    
    try:
        os.rename(categDirPath, categDirPathWithTimestamp)
    except (OSError, FileExistsError, IsADirectoryError, NotADirectoryError) as e:
        logging.critical("Unable to rename file: %s", e)
        raise e

    logging.debug("Renamed %s to %s", filenameAndExtension, categDirPathWithTimestamp)
    

    return

"""
Gets the version of the node from specified file
path: string
    the path of the specified file (usually the system_command file)
return: string
    the version if found. Otherwise, returns 'unknown'
"""
def get_storage_grid_version(path):
    try:
        searchfile = open(path, "r")
        for line in searchfile:
            if "storage-grid-release-" in line:
                searchfile.close()
                return line[21: -1]
        searchfile.close()
        return 'unknown'
    except:
        return 'unknown'


# TODO: implementation
def get_platform(path):
    return 'unknown'

"""
Check if a file is StorageGRID file
"""
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

