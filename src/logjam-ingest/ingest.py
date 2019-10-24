"""
@author Renata Ann Zeitler
@author Josh Good
@author Jeremy Schmidt
@author Nathaniel Brooks

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

from conans import tools
from pyunpack import Archive

import incremental
import utils


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

    if args.scratch_space is not None:
        scratch_dir = os.path.join(os.path.abspath(args.scratch_space),"scratch-space/")
    else:
        scratch_dir = os.path.join(intermediate_dir, "scratch-space/")

    if not os.path.exists(scratch_dir):
        os.makedirs(scratch_dir)
    elif not os.path.isdir(scratch_dir):
        parser.print_usage()
        print('output_directory is not a directory')
        sys.exit(1)

    # Should not allow configuration of intermediate directory
    categ_dir = os.path.join(intermediate_dir, "logjam-categories")
    history_file = os.path.join(intermediate_dir, "scan-history.txt")

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
    ingest_log_files(args.ingestion_directory, categ_dir, scratch_dir, history_file)
    if graceful_abort:
        logging.info("Graceful abort successful")
    else:
        logging.info("Finished ingesting")
    
    logging.info("Cleaning up scratch space")
    utils.delete_directory(scratch_dir)


def ingest_log_files(input_dir, categ_dir, scratch_dir, history_file):
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
        if os.path.isdir(full_path) and entity != ".DS_Store":
            searchAnInspectionDirectory(scan, full_path, categ_dir, scratch_dir)
        else:
            logging.debug("Ignored non-StorageGRID file: %s", full_path)
    
    if graceful_abort:
        scan.premature_exit()
    else:
        scan.complete_scan()
        
    return


def searchAnInspectionDirectory(scan, start, categ_dir, scratch_dir, depth=None, case_num=None):
    """
    Recursively go through directories to find log files. If compressed, then we need
    to unzip/unpack them. Possible file types include: .zip, .gzip, .tar, .tgz, and .7z
    start : string
        the start of the file path to traverse
    depth : string
        the sub-directories and sub-files associated with this directory
    """
    if graceful_abort:
        return

    if not depth:
        depth = ""

    assert os.path.isdir(os.path.join(start, depth)), "This is not a directory: "+os.path.join(start, depth)

    # Loop over each file in the current directory
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
        if case_num == None: case_num = getCaseNumber(entity_path)
        assert case_num != "0", "Not a valid case number: "+case_num
        # Get category
        category = getCategory(entity_path.lower())
          
        if os.path.isdir(entity_path):
            # Detected a directory, continue
            searchAnInspectionDirectory(scan, start, categ_dir, scratch_dir, depth=os.path.join(depth, entity), case_num=case_num)
        
        elif os.path.isfile(entity_path) and scan.should_consider_file(entity_path):    
            if extension in validExtensions or filename in validFiles:
                stash_file_in_elk(entity_path, entity, case_num, categ_dir, False)
            
            elif extension in validZips:
                def handle_unzipped_file(path):
                  # TODO: Change to conditional function
                  # TODO: if is_storagegrid(path):
                  (name,ext) = os.path.splitext(path)
                  if ext in validExtensions or os.path.basename(name) in validFiles:
                    stash_file_in_elk(path, os.path.basename(path), case_num, categ_root, True)
                  else:
                    utils.delete_file(path)
                    logging.debug("Ignored non-StorageGRID file: %s", path)
                  return
                
                # TODO: Choose unique folder names per Logjam worker instance
                # TODO: new_scratch_dir = new_unique_scratch_folder()
                new_scratch_dir = os.path.join(scratch_dir, "tmp")
                os.makedirs(new_scratch_dir)
                utils.recursive_unzip(entity_path, new_scratch_dir, handle_unzipped_file)
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
        
        scan.just_scanned_this_path(entity_path)


def stash_file_in_elk(fullPath, filenameAndExtension, case_num, categ_dir, is_owned):
    """
    Stashes file in ELK stack; checks if duplicate, computes important
    fields like log category, and prepares for ingest by Logstash.
    fullPath : string
        absolute path of the file
    filenameAndExtension : string
        filename + extension of the file, precomputed before function call
    case_num : string
        StorageGRID case number for this file
    categ_dir : string
        directory to stash the file into for Logstash
    is_owned : boolean
        indicates whether the Logjam system owns this file (i.e. can move/delete it)
    """

    assert os.path.isfile(fullPath), "This is not a file: "+fullPath
    assert os.path.basename(fullPath) == filenameAndExtension, "Computed filename+extension doesn't match '"+filename+"' - '"+fullPath+"'"
    assert os.path.splitext(filenameAndExtension)[1] in validExtensions or os.path.splitext(filenameAndExtension)[0] in validFiles, "Not a valid file: "+filenameAndExtension

    # Log in the database and copy to the appropriate logjam category
    if case_num == None: case_num = getCaseNumber(fullPath)
    assert case_num != None, "Null reference"
    assert case_num != "0", "Not a valid case number: "+case_num

    category = getCategory(fullPath.lower())
    assert category != None, "Null reference"

    if not os.path.exists(categ_dir):
        os.makedirs(categ_dir)

    category_dir = os.path.join(categ_dir, category)
    if not os.path.exists(category_dir):
        os.makedirs(category_dir)

    categDirPath = os.path.join(categ_dir, category, filenameAndExtension)

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
    basename = "-".join([case_num, filenameAndExtension, timestamp])
    categDirPathWithTimestamp = os.path.join(categ_dir, category, basename)

    try:
        os.rename(categDirPath, categDirPathWithTimestamp)
    except (OSError, FileExistsError, IsADirectoryError, NotADirectoryError) as e:
        logging.critical("Unable to rename file: %s", e)
        raise e

    logging.debug("Renamed %s/%s to %s", category, filenameAndExtension, categDirPathWithTimestamp)
    logging.debug("Adding %s to Logstash", fullPath)

    return


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


def getCaseNumber(path):
    """
    Extracts the relevant StorageGRID case number from the file's path.
    path : string
        the path to search for case number
    return : string
        the case number found in the path
    """
    caseNum = re.search(r"(\d{10})", path)
    if caseNum is None:
        caseNum = "0"
    else:
        caseNum = caseNum.group()
    return caseNum


if __name__ == "__main__":
    main()

