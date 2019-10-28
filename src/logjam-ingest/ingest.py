"""
@author Renata Ann Zeitler
@author Josh Good
@author Jeremy Schmidt
@author Nathaniel Brooks
@author Wenting Zheng
This script will be used to recursively search through and unzip directories as necessary
and output files with extensions .log and .txt to Logjam
Terminology:
  Inspection Directory - the original directory ingest.py searches through, it should
                         be treated as read-only
  Scratchspace Directory - a directory that ingest.py unzips compressed files into, owned
                           by ingest.py (can R/W there)
  Category Directory - the final directories where ingest.py copies/places files for
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


'''
Recursively walks the directories of the inspection
directory, copying relevant files into Logjam controlled
filespace for further processing by Logstash. Unzips compressed
files into Logjam controlled scratchspace, then moves relevant files
for further processing by Logstash.
'''

def main():
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

    if args.scratch_space is not None:
        scratchDirRoot = os.path.join(os.path.abspath(args.scratch_space),"scratch-space/")
    else:
        scratchDirRoot = os.path.join(intermediate_dir, "scratch-space/")

    if not os.path.exists(scratchDirRoot):
        os.makedirs(scratchDirRoot)
    elif not os.path.isdir(scratchDirRoot):
        parser.print_usage()
        print('output_directory is not a directory')
        sys.exit(1)

    # Should not allow configuration of intermediate directory
    categDirRoot = os.path.join(intermediate_dir, "logjam-categories")
    history_file = os.path.join(intermediate_dir, "scan-history.txt")

    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.WARNING)
    es = Elasticsearch([es_host], verify_certs = True)
    if not es.ping():
        logging.critical("Unable to connect to Elasticsearch")

    log_format = "%(asctime)s %(filename)s line %(lineno)d %(levelname)s %(message)s"
    logging.basicConfig(format=log_format, datefmt="%Y-%m-%d %H:%M:%S", level=args.log_level)

    def signal_handler(signum, frame):
        if signum == signal.SIGINT:
            logging.info("Gracefully aborting")
            global graceful_abort
            graceful_abort = True
    signal.signal(signal.SIGINT, signal_handler)

    # Ingest the directories
    logging.debug("Ingesting %s", args.ingestion_directory)
    ingest_log_files(args.ingestion_directory, categDirRoot, scratchDirRoot, history_file, es)
    if graceful_abort:
        logging.info("Graceful abort successful")
    else:
        logging.info("Finished ingesting")
    
    logging.info("Cleaning up scratch space")
    utils.delete_directory(scratchDirRoot)


def ingest_log_files(input_root, output_root, scratch_space, history_file, es = None):
    """ Begins ingesting files from the specified directories. Assumes that
    Logjam DOES NOT own `input_root` or `output_root` but also assumes that
    Logjam DOES own `scratch_space` and `history_file`.
    """
    assert os.path.isdir(input_root), "Input must exist & be a directory"
    
    scan = incremental.Scan(input_root, history_file)
    
    entities = sorted(os.listdir(input_root))
    for e in range(len(entities)):
        if e+1 != len(entities) and os.path.join(input_root, entities[e+1]) < scan.last_path:
            continue                                    # skip, haven't reached last_path
        
        entity = entities[e]
        full_path = os.path.join(input_root,entity)
        if os.path.isdir(full_path) and entity != ".DS_Store":
            searchAnInspectionDirectory(scan, full_path, output_root, scratch_space, es)
        else:
            logging.debug("Ignored non-StorageGRID file: %s", full_path)
    
    if graceful_abort:
        scan.premature_exit()
    else:
        scan.complete_scan()
        
    return


"""
Recursively go through directories to find log files. If compressed, then we need
to unzip/unpack them. Possible file types include: .zip, .gzip, .tar, .tgz, and .7z
start : string
    the start of the file path to traverse
depth : string
    the sub-directories and sub-files associated with this directory
"""
def searchAnInspectionDirectory(scan, start, output_root, scratch_space, es, depth=None, caseNum=None, scan_dir=None):
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
    if caseNum == None: caseNum = getCaseNumber(search_dir)
    assert caseNum != "0", "Not a valid case number: "+caseNum
    for e in range(len(entities)):
        if e+1 != len(entities) and os.path.join(search_dir, entities[e+1]) < scan.last_path:
            continue                                    # skip, haven't reached last_path

        fileOrDir = entities[e]
        # Check for the file type to make sure it's not compressed
        filename, extension = os.path.splitext(fileOrDir)
        # Get the file's path in inspection dir
        inspecDirPath = os.path.join(search_dir, fileOrDir)
        # Get category
        category = getCategory(inspecDirPath.lower()) 

        if os.path.isdir(inspecDirPath):
            # Detected a directory, continue
            if os.path.isfile(os.path.join(inspecDirPath, 'lumberjack.log')):
                index.stash_node_in_elk(inspecDirPath , caseNum, output_root, False, es)
            else:
                searchAnInspectionDirectory(scan, start, output_root, scratch_space, es, os.path.join(depth, fileOrDir), caseNum, scan_dir)
        elif os.path.isfile(inspecDirPath) and scan.should_consider_file(inspecDirPath):
            if (extension in validExtensions or filename in validFiles) and is_storagegrid(inspecDirPath, ''):
                index.stash_file_in_elk(inspecDirPath, fileOrDir, caseNum, output_root, False, es)
            elif extension in validZips:
                # TODO: Choose unique folder names per Logjam worker instance
                # TODO: new_scratch_dir = new_unique_scratch_folder()
                new_scratch_dir = os.path.join(scratch_space, "tmp")
                os.makedirs(new_scratch_dir)
                utils.recursive_unzip(inspecDirPath, new_scratch_dir)
                f, e = os.path.splitext(fileOrDir)
                unzip_folder = os.path.join(new_scratch_dir, os.path.basename(f.replace('.tar', '')))
                if os.path.isdir(unzip_folder):
                    searchAnInspectionDirectory(scan, unzip_folder, output_root, scratch_space, es, None, caseNum, inspecDirPath)
                elif os.path.isfile(unzip_folder) and (e in validExtensions or os.path.basename(f) in validFiles) and is_storagegrid(unzip_folder, ''):
#                         random_files.append(unzip_folder)
                    index.stash_file_in_elk(unzip_folder, os.path.basename(unzip_folder), caseNum, output_root, True, es)
                assert os.path.exists(inspecDirPath), "Should still exist"
                assert os.path.exists(new_scratch_dir), "Should still exist"
                utils.delete_directory(new_scratch_dir)

                logging.debug("Added compressed archive to DB & ELK: %s", inspecDirPath)
            else:
                # Invalid file, continue
                logging.debug("Assumming incorrect filetype: %s", inspecDirPath)
        else:
            # Previously ingested, continue
            logging.debug("Already ingested %s", inspecDirPath)
        if 'tmp' in inspecDirPath:
            scan.just_scanned_this_path(scan_dir)
        else:
            scan.just_scanned_this_path(inspecDirPath)


"""
Check if a file is StorageGRID file
"""
def is_storagegrid(fullpath, path):
    if 'bycast' in path or 'bycast' in fullpath:
        return True
    else:
        try:
            searchfile = open(os.path.join(fullpath,path), "r")
        except:
            searchfile = open(fullpath, "r")
        for line in searchfile:
            if "bycast" in line:
                searchfile.close()
                return True
        searchfile.close()
    return False
#         open(path, 'r').read().find('bycast')

# TODO: implementation
def get_platform(path):
    return 'unknown'


"""
Gets the category for this file based on path
path : string
    the path for which to get a category
filename : string
    the file's name
"""
def getCategory(path):
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

'''
Extracts the relevant StorageGRID case number from the file's path.
path : string
    the path to search for case number
return : string
    the case number found in the path
'''
def getCaseNumber(path):
    caseNum = re.search(r"(\d{10})", path)
    if caseNum is None:
        caseNum = "0"
    else:
        caseNum = caseNum.group()
    return caseNum


if __name__ == "__main__":
    main()
