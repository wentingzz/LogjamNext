"""
@author Renata Ann Zeitler
@author Josh Good
@author Jeremy Schmidt
@author Nathaniel Brooks
@author Wenting Zheng

This script will be used to recursively search through and unzip directories as necessary
and output files with extensions .log and .txt to Logjam

Terminology:
  Input Directory       - the original directory ingest.py searches through, it should
                          be treated as read-only
  Scratch Directory     - a directory that ingest.py unzips compressed files into, owned
                          by ingest.py (can R/W there)
  Category Directory    - the final directories where ingest.py copies/places files for
                          Logstash to consume, owned by ingest.py (can R/W there)
"""


import argparse
import gzip
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import signal
import concurrent.futures
import multiprocessing

from elasticsearch import Elasticsearch, helpers
from conans import tools
from pyunpack import Archive

import incremental
import unzip
import index
import fields


code_src_dir = os.path.dirname(os.path.realpath(__file__))          # remove eventually
intermediate_dir = os.path.join(code_src_dir, "..", "..", "data")   # remove eventually
MAX_WORKERS = None

mappings_path = os.path.join(code_src_dir, "..", "elasticsearch/mappings.json")

# Valid extensions to ingest
validExtensions = [".txt", ".log"]
# Valid extentionless files used in special cases
validFiles = ["syslog", "messages", "system_commands"]

graceful_abort = False
#elasticsearch host
es_host = 'http://localhost:9200/'

def main():
    """
    Recursively walks the directories of the inspection
    directory, copying relevant files into Logjam controlled
    filespace for further processing by Logstash. Unzips compressed
    files into Logjam controlled scratchspace, then moves relevant files
    for further processing by Logstash. This function validates parameters, then
    starts the main business logic by calling `ingest_log_files`.
    """
    parser = argparse.ArgumentParser(description='File ingestion frontend for Logjam.Next')
    parser.add_argument('--log-level', dest='log_level', default='DEBUG',
                        help='log level of script: DEBUG, INFO, WARNING, or CRITICAL')
    parser.add_argument(dest='ingestion_directory', action='store',
                        help='Directory to ingest files from')
    parser.add_argument('-o', '--output-dir', dest='output_directory', action='store',
                        help='Directory to output StorageGRID files to')
    parser.add_argument('-s', '-scratch-space-dir', dest='scratch_space', action='store',
                        help='Scratch space directory to unzip files into')
    args = parser.parse_args()

    if not os.path.isdir(args.ingestion_directory):
        parser.print_usage()
        print('ingestion_directory is not a directory')
        sys.exit(1)

    tmp_scratch_folder = '-'.join(["scratch-space",str(int(time.time()))])+'/'
    if args.scratch_space is not None:
        scratch_dir = os.path.join(os.path.abspath(args.scratch_space), tmp_scratch_folder)
    else:
        scratch_dir = os.path.join(intermediate_dir, tmp_scratch_folder)

    if not os.path.exists(scratch_dir):
        os.makedirs(scratch_dir)
    elif not os.path.isdir(scratch_dir):
        parser.print_usage()
        print('output_directory is not a directory')
        sys.exit(1)

    log_format = "%(asctime)s %(process)d %(filename)s:%(lineno)d %(levelname)s %(message)s"
    logging.basicConfig(format=log_format, datefmt="%b-%d %H:%M:%S", level=args.log_level)

    # Should not allow configuration of intermediate directory
    history_dir = os.path.join(intermediate_dir, "scan-history")

    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)


    def signal_handler(signum, frame):
        if signum == signal.SIGINT:
            logging.info("Gracefully aborting")
            global graceful_abort
            graceful_abort = True
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Ingest the directories
        logging.debug("Ingesting %s", args.ingestion_directory)
        ingest_log_files(args.ingestion_directory, scratch_dir, history_dir)
        if graceful_abort:
            logging.info("Graceful abort successful")
        else:
            logging.info("Finished ingesting")
    
    except Exception as e:
        raise e
    
    finally:
        logging.info("Cleaning up scratch space")
        unzip.delete_directory(scratch_dir)         # always delete scratch_dir

def get_es_connection():
    es = Elasticsearch([es_host], verify_certs = True)
    if not es.ping():
        logging.critical("Unable to connect to Elasticsearch")
        return None
    elif not es.indices.exists(index.INDEX_NAME):
        with open(mappings_path) as mappings_file:
            mappings = mappings_file.read()
        logging.info("Index %s did not exist. Creating.", index.INDEX_NAME)
        es.indices.create(index.INDEX_NAME, body=mappings)
    return es

def ingest_log_files(input_dir, scratch_dir, history_dir):
    """
    Begins ingesting files from the specified directories. Assumes that
    Logjam DOES NOT own `input_dir` or `categ_dir` but also assumes that
    Logjam DOES own `scratch_dir` and `history_dir`.
    """
    assert os.path.isdir(input_dir), "Input must exist & be a directory"
    
    scan = incremental.ManagerScan(input_dir, history_dir, scratch_dir)
    
    try:
        entities = os.listdir(input_dir)
    except OSError as e:
        logging.critical("Error during os.listdir(%s): %s", input_dir, e)
        entities = []
    entities = sorted(entities)
    lock = multiprocessing.Manager().Lock()
    
    with concurrent.futures.ProcessPoolExecutor(max_workers = MAX_WORKERS) as executor:
        for e in range(len(entities)):
            if e != len(entities) and os.path.join(input_dir, entities[e]) <= scan.last_path:
                continue                                # skip, haven't reached last_path

            entity = entities[e]
            full_path = os.path.join(input_dir,entity)
            if os.path.isdir(full_path):
                case_num = fields.get_case_number(entity)
                if case_num != None:
                    executor.submit(search_case_directory, scan, full_path, case_num)
                else:
                    logging.debug("Ignored non-StorageGRID directory: %s", full_path)
            else:
                logging.debug("Ignored non-StorageGRID file: %s", full_path)
    if graceful_abort:
        scan.premature_exit()
    else:
        scan.complete_scan()
    return


def search_case_directory(scan_obj, search_dir, case_num):
    """
    Searches the specified case directory for StorageGRID log files which have not
    been indexed by the Logjam system. Uses the Scan object's time period window to
    determine if a file has been previously indexed. Upon finding valid files, will
    send them to a running Elastissearch service via the Elastisearch object `es_obj`.
    """
    child_scan = incremental.WorkerScan(search_dir, scan_obj.history_dir, scan_obj.scratch_dir, str(case_num) + ".txt", str(case_num) + "-log.txt", scan_obj.safe_time)
    es_obj = get_es_connection()
    recursive_search(child_scan, search_dir, es_obj, case_num)
    global graceful_abort
    if graceful_abort:
        child_scan.premature_exit()
    else:
        child_scan.complete_scan()
        unzip.delete_file(child_scan.history_log_file)
    return


def recursive_search(scan, start, es, case_num, depth=None, scan_dir=None):
    """
    Recursively go through directories to find log files. If compressed, then we need
    to unzip/unpack them. Possible file types include: .zip, .gzip, .tar, .tgz, and .7z
    start : string
        the start of the file path to traverse
    depth : string
        the sub-directories and sub-files associated with this directory
    case_num : string
        string for the case number of this case directory
    """
    assert case_num != None, "Case number must be provided"
    
    if graceful_abort:
        return

    if not depth:
        depth = ""
    
    if not scan_dir:
        scan_dir = start

    assert os.path.isdir(os.path.join(start, depth)), "This is not a directory: "+os.path.join(start, depth)
    
    # Loop over each file in the current directory\
    search_dir = os.path.join(start, depth)
    
    try:
        entities = os.listdir(search_dir)
    except OSError as e:
        logging.critical("Error during os.listdir(%s): %s", search_dir, e)
        entities = []
    entities = sorted(entities)
    
    for e in range(len(entities)):
        if e+1 != len(entities) and os.path.join(search_dir, entities[e+1]) < scan.last_path:
            continue                                    # skip, haven't reached last_path

        entity = entities[e]
        
        # Check for the file type to make sure it's not compressed
        filename, extension = os.path.splitext(entity)
        # Get the file's path in inspection dir
        entity_path = os.path.join(search_dir, entity)

        if os.path.isdir(entity_path):
            if os.path.isfile(os.path.join(entity_path, 'lumberjack.log')):
                process_node(entity_path , case_num, es)
            else:
                # Detected a directory, continue
                recursive_search(scan, start, es, case_num, os.path.join(depth, entity), scan_dir)
        
        elif os.path.isfile(entity_path):
            # Check timespan of scan (already scanned?)
            if not scan.should_consider_file(entity_path):
                logging.debug("Skipping file %s outside scan timespan", entity_path)
            # Case for regular file. Check for relevance, then ingest.
            elif extension in validExtensions or filename in validFiles:
                if fields.is_storagegrid(entity_path):
                    process_unknown_file(entity_path, case_num, es)
                else:
                    logging.debug("Skipping non-storagegrid file %s", entity_path)
            # Case for archive. Recursively unzip and ingest contents.
            elif extension in unzip.SUPPORTED_FILE_TYPES:
                # TODO: Choose unique folder names per Logjam worker instance
                # TODO: new_scratch_dir = new_unique_scratch_folder()
                new_scratch_dir = os.path.join(scan.scratch_dir, "tmp" + str(case_num))
                os.makedirs(new_scratch_dir)
                unzip.recursive_unzip(entity_path, new_scratch_dir)
                f, e = os.path.splitext(entity)
                unzip_folder = os.path.join(new_scratch_dir, os.path.basename(f.replace('.tar', '')))
                if os.path.isdir(unzip_folder):
                    recursive_search(scan, unzip_folder, es, case_num, None, entity_path)
                elif os.path.isfile(unzip_folder) and (e in validExtensions or os.path.basename(f) in validFiles) and fields.is_storagegrid(unzip_folder):
#                         random_files.append(unzip_folder)
                    process_unknown_file(unzip_folder, case_num, es)
                
                assert os.path.exists(entity_path), "Should still exist"
                assert os.path.exists(new_scratch_dir), "Should still exist"
                unzip.delete_directory(new_scratch_dir)

                logging.debug("Added compressed archive to ELK: %s", entity_path)
            else:
                # Invalid file, continue
                logging.debug("Skipping file %s with extension %s", entity_path, extension)
        else:
            # Previously ingested, continue
            logging.debug("Already ingested %s", entity_path)
        
        if 'tmp' in entity_path:
            scan.just_scanned_this_path(scan_dir)
        else:
            scan.just_scanned_this_path(entity_path)


def process_node(lumber_dir, case_num, es = None):
    """ Stashes a node in ELK stack;
    lumber_dir : string
        absolute path of the node
    case_num : string
        StorageGRID case number for this file
    es: Elasticsearch
        Elasticsearch instance
    """
    assert case_num != None, "Null reference"
    assert case_num != "0", "Not a valid case number: "+case_num
    
    files = process_node_recursive(lumber_dir, [])
    nodefields = fields.extract_fields(lumber_dir, inherit_from=fields.NodeFields(case_num=case_num))
    if es:
        for file in files:
            index.send_to_es(es, nodefields, file)
    
    return


def process_node_recursive(lumber_dir, file_list):
    """ Finds all the files in the node; returns all the content as a array
    lumber_dir : string
        absolute path of the node
    file_list: array of string
        array of the content. Each element is the content of a file in the node
    """
    for entry in os.listdir(lumber_dir):
        entry_path = os.path.join(lumber_dir, entry)
        name, extension = os.path.splitext(entry)
        
        if os.path.isfile(entry_path) and (extension in validExtensions or name in validFiles) and fields.is_storagegrid(entry_path):
            file_list.append(entry_path)
        
        elif os.path.isdir(entry_path):
            process_node_recursive(entry_path, file_list)
    
    return file_list


def process_unknown_file(file_path, case_num, es = None):
    """ Stashes file in ELK stack; checks if duplicate, computes important
    fields like log category, and prepares for ingest by Logstash.
    file_path : string
        absolute path of the file
    case_num : string
        StorageGRID case number for this file
    es: Elasticsearch
        Elasticsearch instance
    """
    assert os.path.isfile(file_path), "This is not a file: "+file_path
    assert case_num != None, "Null reference"
    assert case_num != "0", "Not a valid case number: "+case_num

    nodefields = fields.NodeFields(case_num=case_num)            # only case for fields
    if es:
        index.send_to_es(es, nodefields, file_path)              # send as unknown node
    
    return


if __name__ == "__main__":
    main()

