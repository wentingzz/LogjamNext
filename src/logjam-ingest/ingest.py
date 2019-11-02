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
import utils
import index


code_src_dir = os.path.dirname(os.path.realpath(__file__))          # remove eventually
intermediate_dir = os.path.join(code_src_dir, "..", "..", "data")   # remove eventually

mappings_path = os.path.join(code_src_dir, "..", "elasticsearch/mappings.json")

# List of all categories to sort log files by
categories = {"audit" : r".*audit.*", "base_os_commands" : r".*base[/_-]*os[/_-]*.*command.*",
              "bycast" : r".*bycast.*", "cassandra_commands" : r".*cassandra[/_-]*command.*",
              "cassandra_gc" : r".*cassandra[/_-]*gc.*",
              "cassandra_system" : r".*cassandra[/_-]*system.*", "dmesg" : r".*dmesg.*",
              "gdu_server" : r".*gdu[/_-]*server.*", "init_sg": r".*init[/_-]*sg.*", "install": r".*install.*",
              "kern" : r".*kern.*", "messages": r".*messages.*", "pge_image_updater": r".*pge[/_-]*image[/_-]*updater.*",
              "pge_mgmt_api" : r".*pge[/_-]*mgmt[/_-]*api.*", "server_manager" : r".*server[/_-]*manager.*",
              "sg_fw_update" : r".*sg[/_-]*fw[/_-]*update.*", "storagegrid_daemon" : r".*storagegrid.*daemon.*",
              "storagegrid_node" : r".*storagegrid.*node.*", "syslog" : ".*syslog.*",
              "system_commands": r".*system[/_-]*commands.*", "upgrade":r".*upgrade.*" }

# Valid extensions to ingest
validExtensions = [".txt", ".log"]
# Valid extentionless files used in special cases
validFiles = ["syslog", "messages", "system_commands"]
# Valid zip formats
validZips = [".gz", ".tgz", ".tar", ".zip", ".7z"]

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
    history_file = os.path.join(intermediate_dir, "scan-history.txt")

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
        ingest_log_files(args.ingestion_directory, scratch_dir, history_file, es)
        if graceful_abort:
            logging.info("Graceful abort successful")
        else:
            logging.info("Finished ingesting")
    
    except Exception as e:
        raise e
    
    finally:
        logging.info("Cleaning up scratch space")
        utils.delete_directory(scratch_dir)         # always delete scratch_dir


def ingest_log_files(input_dir, scratch_dir, history_file, es = None):
    """
    Begins ingesting files from the specified directories. Assumes that
    Logjam DOES NOT own `input_dir` or `categ_dir` but also assumes that
    Logjam DOES own `scratch_dir` and `history_file`.
    """
    assert os.path.isdir(input_dir), "Input must exist & be a directory"
    
    scan = incremental.Scan(input_dir, history_file)
    
    entities = sorted(os.listdir(input_dir))
    for e in range(len(entities)):
        if e+1 != len(entities) and os.path.join(input_dir, entities[e+1]) < scan.last_path:
            continue                                    # skip, haven't reached last_path
        
        entity = entities[e]
        full_path = os.path.join(input_dir,entity)
        if os.path.isdir(full_path):
            case_num = extract_case_number(entity)
            if case_num != None:
                search_case_directory(scan, full_path, scratch_dir, es, case_num)
            else:
                logging.debug("Ignored non-StorageGRID directory: %s", full_path)
        else:
            logging.debug("Ignored non-StorageGRID file: %s", full_path)
    
    if graceful_abort:
        scan.premature_exit()
    else:
        scan.complete_scan()
        
    return


def search_case_directory(scan_obj, search_dir, scratch_dir, es_obj, case_num):
    """
    Searches the specified case directory for StorageGRID log files which have not
    been indexed by the Logjam system. Uses the Scan object's time period window to
    determine if a file has been previously indexed. Upon finding valid files, will
    send them to a running Elastissearch service via the Elastisearch object `es_obj`.
    """
    return recursive_search(scan_obj, search_dir, scratch_dir, es_obj, case_num)

def recursive_search(scan, start, scratch_dir, es, case_num, depth=None, scan_dir=None):
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
    entities = sorted(os.listdir(search_dir))
    for e in range(len(entities)):
        if e+1 != len(entities) and os.path.join(search_dir, entities[e+1]) < scan.last_path:
            continue                                    # skip, haven't reached last_path

        entity = entities[e]
        
        # Check for the file type to make sure it's not compressed
        filename, extension = os.path.splitext(entity)
        # Get the file's path in inspection dir
        entity_path = os.path.join(search_dir, entity)
        # Get category
        category = getCategory(entity_path.lower())

        if os.path.isdir(entity_path):
            if os.path.isfile(os.path.join(entity_path, 'lumberjack.log')):
                index.stash_node_in_elk(entity_path , case_num, es)
            else:
                # Detected a directory, continue
                recursive_search(scan, start, scratch_dir, es, case_num, os.path.join(depth, entity), scan_dir)
        
        elif os.path.isfile(entity_path) and scan.should_consider_file(entity_path):
            if (extension in validExtensions or filename in validFiles) and index.is_storagegrid(entity_path):
                index.stash_file_in_elk(entity_path, entity, case_num, es)
            elif extension in validZips:
                # TODO: Choose unique folder names per Logjam worker instance
                # TODO: new_scratch_dir = new_unique_scratch_folder()
                new_scratch_dir = os.path.join(scratch_dir, "tmp")
                os.makedirs(new_scratch_dir)
                utils.recursive_unzip(entity_path, new_scratch_dir)
                f, e = os.path.splitext(entity)
                unzip_folder = os.path.join(new_scratch_dir, os.path.basename(f.replace('.tar', '')))
                if os.path.isdir(unzip_folder):
                    recursive_search(scan, unzip_folder, scratch_dir, es, case_num, None, entity_path)
                elif os.path.isfile(unzip_folder) and (e in validExtensions or os.path.basename(f) in validFiles) and index.is_storagegrid(unzip_folder):
#                         random_files.append(unzip_folder)
                    index.stash_file_in_elk(unzip_folder, os.path.basename(unzip_folder), case_num, es)
                
                assert os.path.exists(entity_path), "Should still exist"
                assert os.path.exists(new_scratch_dir), "Should still exist"
                utils.delete_directory(new_scratch_dir)

                logging.debug("Added compressed archive to ELK: %s", entity_path)
            else:
                # Invalid file, continue
                logging.debug("Assumming incorrect filetype: %s", entity_path)
        else:
            # Previously ingested, continue
            logging.debug("Already ingested %s", entity_path)
        
        if 'tmp' in entity_path:
            scan.just_scanned_this_path(scan_dir)
        else:
            scan.just_scanned_this_path(entity_path)


def getCategory(path):
    """
    Gets the category for this file based on path
    path : string
        the path for which to get a category
    filename : string
        the file's name
    """
    # Split the path by sub-directories
    splitPath = path.replace('\\','/').split("/")
    start = splitPath[len(splitPath) - 1]
    splitPath.pop()
    # For each part in this path, run each category regex expression
    # and return the first match
    for part in reversed(splitPath):
        for cat, regex in categories.items():
            if re.search(regex, start):
                return cat
        start = os.path.join(part, start)

    # Unrecognized file, so return "other"
    return "other"


def extract_case_number(dir_name):
    """
    Extracts the StorageGRID case number from a directory name.
    dir_name : string
        the directory name to search for case number
    return : string
        the case number that was found or None is nothing was found
    """
    match_obj = re.match(r"^(\d{10})$", dir_name)
    if match_obj is None:
        return None
    else:
        return match_obj.group()


if __name__ == "__main__":
    main()

