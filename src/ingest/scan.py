"""
@author Renata Ann Zeitler
@author Josh Good
@author Jeremy Schmidt
@author Nathaniel Brooks
@author Wenting Zheng
@author Daniel Grist

This script will be used to recursively search through and unzip directories as necessary
and output files with extensions .log and .txt to Logjam

Terminology:
  Input Directory       - the original directory scan.py searches through, it should
                          be treated as read-only
  Scratch Directory     - a directory that scan.py unzips compressed files into, owned
                          by scan.py (can R/W there)
  Category Directory    - the final directories where scan.py copies/places files for
                          Logstash to consume, owned by scan.py (can R/W there)
"""


import argparse
import logging
import os
import sys
import time
import signal
import concurrent.futures
from tqdm import tqdm
import multiprocessing

from elasticsearch import Elasticsearch

import incremental
import unzip
import index
import fields
import paths

# Directory of the code source
code_src_dir = os.path.dirname(os.path.realpath(__file__))

# Data directory
intermediate_dir = os.path.join(code_src_dir, "..", "..", "data")

# Max number of workers (None = number of processors)
MAX_WORKERS = None

# Path to the Elasticsearch mappings
mappings_path = os.path.join(code_src_dir, "..", "elasticsearch/mappings.json")

# Flag used for aborting in the middle of the scan
graceful_abort = False

# Elasticsearch host
es_host = "http://%s:9200" % os.environ.get("ELASTICSEARCH_HOST", "localhost")

LOG_LEVEL_STRS = {
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "CRITICAL": logging.CRITICAL,
    "DEBUG": logging.DEBUG,
}


def main():
    """
    Recursively walks the directories of the inspection
    directory. Unzips compressed files into Logjam controlled scratchspace. 
    This function validates parameters, then
    starts the main business logic by calling `ingest_log_files`.
    """
    parser = argparse.ArgumentParser(description='File ingestion frontend for Logjam.Next')
    parser.add_argument('--log-level', dest='log_level', default="DEBUG",
                        help='log level of script: DEBUG, INFO, WARNING, or CRITICAL')
    parser.add_argument(dest='input_dir', action='store',
                        help='Directory to scan for StorageGRID files')
    parser.add_argument('-o', '--output-dir', dest='output_directory', action='store',
                        help='Directory to output StorageGRID files to')
    parser.add_argument('-s', '-scratch-space-dir', dest='scratch_space', action='store',
                        help='Scratch space directory to unzip files into')
    parser.add_argument('-p','--processor',dest='processor_num',type=int,help='Processor number')
    args = parser.parse_args()

    log_level = LOG_LEVEL_STRS.get(args.log_level, "DEBUG")
    log_format = "%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s"
    logging.basicConfig(format=log_format, datefmt="%b-%d %H:%M:%S", level=log_level)

    args.input_dir = os.path.normpath(args.input_dir)
    if not os.path.isdir(args.input_dir):
        parser.print_usage()
        print('input_dir is not a directory')
        sys.exit(1)

    get_es_connection()
    
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

    # Should not allow configuration of intermediate directory
    history_dir = os.path.join(intermediate_dir, "scan-history")

    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)

    global MAX_WORKERS
    MAX_WORKERS = args.processor_num

    def signal_handler(signum, frame):
        if signum == signal.SIGINT:
            logging.info("Gracefully aborting")
            global graceful_abort
            graceful_abort = True
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # ingest_log_files from the input directory
        logging.debug("Ingesting: %s", args.input_dir)
        ingest_log_files(args.input_dir, scratch_dir, history_dir)
        if graceful_abort:
            logging.info("Graceful abort successful")
        else:
            logging.info("Finished ingesting")
    
    except Exception as e:
        raise e
    
    finally:
        logging.info("Cleaning up scratch space")
        # Always delete scratch_dir
        unzip.delete_directory(scratch_dir)     


def get_es_connection():
    es = Elasticsearch([es_host], verify_certs = True)
    if not es.ping():
        raise Exception("Unable to connect to Elasticsearch")
    elif not es.indices.exists(index.INDEX_NAME):
        with open(mappings_path) as mappings_file:
            mappings = mappings_file.read()
        logging.info("Index %s did not exist. Creating.", index.INDEX_NAME)
        es.indices.create(index.INDEX_NAME, body=mappings)
    return es


def ingest_log_files(input_dir, scratch_dir, history_dir):
    """
    Begins ingesting files from the specified directories. Assumes that
    Logjam DOES NOT own `input_dir` but also assumes that
    Logjam DOES own `scratch_dir` and `history_dir`.
    input_dir: string
        path to the input directory
    scratch_dir: string
        path to the scratch directory
    history_dir: string
        path to the histry directory
    """
    assert os.path.isdir(input_dir), "Input must exist & be a directory"
    
    scan = incremental.ManagerScan(input_dir, history_dir, scratch_dir)
    assert os.path.exists(scan.history_log_file)
    
    with concurrent.futures.ProcessPoolExecutor(max_workers = MAX_WORKERS) as executor:
        
        futures = []
        search_dir = paths.QuantumEntry(input_dir, "")
        for e in incremental.list_unscanned_entries(search_dir,os.path.basename(scan.last_path)):
            
            if e.is_dir():
                case_num = fields.get_case_number(e.relpath)
                if case_num != fields.MISSING_CASE_NUM:
                    logging.debug("Search case directory: %s", e.abspath)
                    futures.append(executor.submit(search_case_directory, scan, input_dir, case_num))
                    
                    assert os.path.exists(scan.history_log_file), "History Log File does not exist for case: "+case_num
                    
                else:
                    logging.debug("Ignored non-StorageGRID directory: %s", e.abspath)
            else:
                logging.debug("Ignored non-StorageGRID file: %s", e.abspath)

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            # Raise any exception from child process
            future.result()
    
    if graceful_abort:
        scan.premature_exit()
    else:
        scan.complete_scan()
    return


def search_case_directory(scan_obj, input_dir, case_num):
    """
    Searches the specified case directory for StorageGRID log files which have not
    been indexed by the Logjam system. Uses the Scan object's time period window to
    determine if a file has been previously indexed. Upon finding valid files, will
    send them to a running Elastissearch service via the Elastisearch object `es_obj`.
    scan_object: ManagerScan
        Used to determine the scan period
    input_dir: string
        path to input directory 
    case_num: string
        case directory number
    """
    
    global graceful_abort
    if graceful_abort:
        return
        
    assert case_num != fields.MISSING_CASE_NUM, "Case number should have already been verified"
    
    child_scan = incremental.WorkerScan(input_dir, scan_obj.history_dir, 
                                        scan_obj.scratch_dir, str(case_num) + ".txt", 
                                        str(case_num) + "-log.txt", scan_obj.safe_time)

    assert child_scan.input_dir == scan_obj.input_dir
    
    if not child_scan.already_scanned:
        es_obj = get_es_connection()
        fields_obj = fields.NodeFields(case_num=case_num)
    
        case_dir = paths.QuantumEntry(scan_obj.input_dir, case_num)
        assert case_dir.exists(), "Case directory does not exist!"
        logging.debug("Recursing into case directory: %s", case_dir.abspath)
        recursive_search(child_scan, es_obj, fields_obj, case_dir)
    
        if graceful_abort:
            child_scan.premature_exit()
        else:
            child_scan.complete_scan()
            unzip.delete_file(child_scan.history_log_file)
    
    return


def recursive_search(scan, es, nodefields, cur_dir):
    """
    Recursively searches directories for StorageGRID Nodes and Log Files. Unzips
    compressed files as needed. Sends the log data to Elasticsearch via the 'es'.
    scan: ManagerScan
        Keeps track of what has been scanned
    es: Elasticsearch object
        Elasticsearch
    nodefields: NodeFields
        contains the NodeFields to be added
    cur_dir: QuantumEntry
        path of the current directory
    """
    assert isinstance(nodefields, fields.NodeFields), "Wrong argument type"
    assert isinstance(cur_dir, paths.QuantumEntry), "Wrong argument type"
    assert cur_dir.is_dir(), "Entry is not a directory: " + cur_dir.abspath
    
    # Exit if abort requested
    if graceful_abort:       
        return

    # Extract fields first
    if (cur_dir/"lumberjack.log").is_file():            
        logging.debug("Extracting fields from lumberjack directory: %s", cur_dir.relpath)
        nodefields = fields.extract_fields(cur_dir.abspath, inherit_from=nodefields)
    
    # Loop over each unscanned entry and ingest it
    for entry in scan.list_unscanned_entries(cur_dir): 
        if not scan.should_consider_entry(entry):       
            logging.debug("Skipping file, outside timespan: %s", entry.abspath)
            # Log the scan
            scan.just_scanned_this_entry(entry)         
            continue                                    
        
        if entry.extension in unzip.SUPPORTED_FILE_TYPES and entry.is_file():
            scratch_entry = unzip_into_scratch_dir(scan.input_dir, scan.scratch_dir, entry)
            if scratch_entry == entry:
                logging.debug("Skipping archive, already unpacked: %s", entry.abspath)
                # Log the scan
                scan.just_scanned_this_entry(entry)     
                continue                                
            else:
                logging.debug("Unpacked archive, path open: %s", scratch_entry.abspath)
                # Override old entry
                entry = scratch_entry
        
        if entry.is_file():
            if fields.is_storagegrid(nodefields, entry):
                index.send_to_es(es, nodefields, entry)
            else:
                logging.debug("Skipped Non-StorageGRID file: %s", entry.abspath)
        
        elif entry.is_dir():
            try:
                logging.debug("Recursing into directory: %s", entry.abspath)
                recursive_search(scan, es, nodefields, entry)
            except OSError as e:
                logging.critical("Could not access directory: %s\nError: %s\nSkipping directory", 
                                 cur_dir.abspath, e)

        # Wasn't a directory or a file
        else:                                           
            logging.debug("Skipped unknown entry: %s", entry.abspath)
        
        if entry.srcpath == scan.scratch_dir:           
            logging.debug("Delete unpacked archive: %s", entry.abspath)
            # rm on FS (does not clear entry)
            entry.delete()                   
        # Log the scan
        scan.just_scanned_this_entry(entry)             
        continue                                        
    
    return                                              


def unzip_into_scratch_dir(input_dir, scratch_dir, compressed_entry):
    """
    Unzips the compressed file into the provided scratch directory. If the file
    has already been decompressed, return the compressed file unchanged. Uses the
    relative path of the file to mimic a structure under the scratch directory.
    For example:
                       Source       - Relative
    compressed_entry = /mnt/srv/nfs - 2001589801/var/os/dir.zip  ---.
       scratch_entry = /tmp/scratch - 2001589801/var/os/dir  <------'

    input_dir: string
        path to the input directory
    scratch_dir: string
        path to the scratch directory
    compressed_entry: QuantumEntry
        directory of the compressed entry
    """
    assert isinstance(input_dir, str)
    assert isinstance(scratch_dir, str)
    assert isinstance(compressed_entry, paths.QuantumEntry)
    assert compressed_entry.is_file(), "Compressed entry should be a file"
    assert compressed_entry.srcpath in [input_dir, scratch_dir], \
        "Source should be input/scratch\nsrcpath: %s\ninput_dir: %s\nscratch_dir: %s" % \
        (compressed_entry.srcpath, input_dir, scratch_dir)
    
    stripped_rel_path = unzip.strip_all_zip_exts(compressed_entry.relpath)
    scratch_entry = paths.QuantumEntry(scratch_dir, stripped_rel_path)
    
    if scratch_entry.exists_in(input_dir) or scratch_entry.exists_in(scratch_dir):
        # Already exists, return unchanged
        return compressed_entry           

    assert not scratch_entry.exists(), "Scratch entry should not exist"
    try:
        unzip.recursive_unzip(compressed_entry.abspath, scratch_entry.absdirpath)
        assert scratch_entry.exists(),"Scratch entry should exist" + scratch_entry.relpath
    except unzip.AcceptableException:
        pass
    # Return unzipped entry
    return scratch_entry   


if __name__ == "__main__":
    main()

