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

from elasticsearch import Elasticsearch, helpers
from conans import tools
from pyunpack import Archive

import incremental
import unzip
import index
import fields
import paths


code_src_dir = os.path.dirname(os.path.realpath(__file__))          # remove eventually
intermediate_dir = os.path.join(code_src_dir, "..", "..", "data")   # remove eventually

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

    log_format = "%(asctime)s %(filename)s line %(lineno)d %(levelname)s %(message)s"
    logging.basicConfig(format=log_format, datefmt="%Y-%m-%d %H:%M:%S", level=args.log_level)

    # Should not allow configuration of intermediate directory
    history_dir = os.path.join(intermediate_dir, "scan-history")

    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    es = Elasticsearch([es_host], verify_certs = True)
    if not es.ping():
        logging.critical("Unable to connect to Elasticsearch")
        es = None
    elif not es.indices.exists(index.INDEX_NAME):
        with open(mappings_path) as mappings_file:
            mappings = mappings_file.read()
        logging.info("Index %s did not exist. Creating.", index.INDEX_NAME)
        es.indices.create(index.INDEX_NAME, body=mappings)


    def signal_handler(signum, frame):
        if signum == signal.SIGINT:
            logging.info("Gracefully aborting")
            global graceful_abort
            graceful_abort = True
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Ingest the directories
        logging.debug("Ingesting %s", args.ingestion_directory)
        ingest_log_files(args.ingestion_directory, scratch_dir, history_dir, es)
        if graceful_abort:
            logging.info("Graceful abort successful")
        else:
            logging.info("Finished ingesting")
    
    except Exception as e:
        raise e
    
    finally:
        logging.info("Cleaning up scratch space")
        unzip.delete_directory(scratch_dir)         # always delete scratch_dir


def ingest_log_files(input_dir, scratch_dir, history_dir, es_obj = None):
    """
    Begins ingesting files from the specified directories. Assumes that
    Logjam DOES NOT own `input_dir` or `categ_dir` but also assumes that
    Logjam DOES own `scratch_dir` and `history_dir`.
    """
    assert os.path.isdir(input_dir), "Input must exist & be a directory"
    
    scan = incremental.Scan(input_dir, history_dir, scratch_dir)
    
    try:
        entities = os.listdir(input_dir)
    except OSError as e:
        logging.critical("Error during os.listdir(%s): %s", input_dir, e)
        entities = []
    entities = sorted(entities)
    
    for e in range(len(entities)):
        if e+1 != len(entities) and os.path.join(input_dir, entities[e+1]) < scan.last_path:
            continue                                    # skip, haven't reached last_path
        
        entity = entities[e]
        full_path = os.path.join(input_dir,entity)
        if os.path.isdir(full_path):
            case_num = fields.get_case_number(entity)
            if case_num != None:
                search_case_directory(scan, full_path, es_obj, case_num)
            else:
                logging.debug("Ignored non-StorageGRID directory: %s", full_path)
        else:
            logging.debug("Ignored non-StorageGRID file: %s", full_path)
    
    if graceful_abort:
        scan.premature_exit()
    else:
        scan.complete_scan()
        
    return


def search_case_directory(scan_obj, case_dir, es_obj, case_num):
    """
    Searches the specified case directory for StorageGRID log files which have not
    been indexed by the Logjam system. Uses the Scan object's time period window to
    determine if a file has been previously indexed. Upon finding valid files, will
    send them to a running Elastissearch service via the Elastisearch object `es_obj`.
    """
    case_num = None                         # will remove after parallelization done
    
    case_dir_entry = paths.QuantumEntry(scan_obj.input_dir, os.path.basename(case_dir))
    
    case_num = fields.get_case_number(case_dir_entry.abspath)
    assert case_num != fields.MISSING_CASE_NUM, "Case number should have already been verified"
    
    fields_obj = fields.NodeFields(case_num=case_num)
    
    recursive_search(scan_obj, es_obj, fields_obj, case_dir_entry)


def recursive_search(scan, es, nodefields, cur_dir):
    """
    Recursively searches directories for StorageGRID Nodes and Log Files. Unzips
    compressed files as needed. Sends the log data to Elasticsearch via the 'es'.
    """
    assert isinstance(nodefields, fields.NodeFields), "Wrong argument type"
    assert isinstance(cur_dir, paths.QuantumEntry), "Wrong argument type"
    assert cur_dir.is_dir(), "Entry is not a directory: " + cur_dir.abspath
    
    if graceful_abort:                                  # kick out if abort requested
        return
    
    if (cur_dir/"lumberjack.log").isfile():             # extract fields 1st
        logging.debug("Extracting fields from lumberjack directory: %s", cur_dir.abspath)
        nodefields = fields.extract_fields(cur_dir.abspath, inherit_from=nodefields)
    
    for entry in scan.list_unscanned_entries(cur_dir):  # loop over each unscanned entry
        
        if scan.should_consider_entry(entry):           # check, has been scanned?
            logging.debug("Skipping file, outside scan timespan: %s", entry.abspath)
            scan.just_scanned_this_entry(entry)         # log the scan
            continue                                    # continue, next entry
        else:
            logging.debug("Considering entry: %s", entry.abspath)
        
        if entry.extension in unzip.SUPPORTED_FILE_TYPES and entry.is_file():
            scratch_entry = paths.QuantumEntry(scan.scratch_dir, entry.relpath)
            scratch_entry = unzip.strip_all_zip_exts(scratch_entry.relpath)
            
            if scratch_entry.exists() or scratch_entry.exists_in(scan.input_dir):
                logging.debug("Skipping archive, already unpacked: %s", entry.abspath)
                scan.just_scanned_this_entry(entry)     # log the scan
                continue                                # continue, next entry
            else:
                logging.debug("Unpacking archive, path open: %s", entry.abspath)
            
            assert not scratch_entry.exists(), "Entry should not be there"
            unzip.recursive_unzip(entry.abspath, scratch_entry.dirname)
            assert scratch_entry.exists(), "Entry should have been created"
            
            entry = scratch_entry                       # override old entry
        
        if entry.is_file():                             # entry is a file
            if entry.extension in validExtensions or entry.filename in validFiles:
                if fields.is_storagegrid(entry.abspath):# check for relevance, then ingest
                    logging.debug("
                    index.send_to_es(es, nodefields, entry.abspath)
                else:
                    logging.debug("Skipping non-storagegrid file %s", entry.abspath)
            else:                                       # bad file extension, skip
                logging.debug("Skipping file, bad extension (%s): %s", entry.extension, entry.abspath)
        
        elif entry.is_dir():                            # detect a directory, continue
            try:
                logging.debug("Recursing into directory: %s", entry.abspath)
                recursive_search(scan, es, nodefields, entry)
            except OSError as e:
                logging.critical("Could not access directory: %s\nError: %s\nSkipping directory", cur_dir.abspath, e)
        
        else:                                           # it wasn't a dir or file?
            logging.debug("Skipping unknown entry: %s", entry.abspath)
        
        if entry.srcpath == scan.scratch_dir:           # if entry inside scratch
            logging.debug("Deleting unpacked archive: %s", entry.abspath)
            entry.delete()                              # rm on FS (does not clear entry)
        
        scan.just_scanned_this_entry(entry)             # log the scan
        continue                                        # continue, next entry
    
    return                                              # done searching this cur_dir


if __name__ == "__main__":
    main()

