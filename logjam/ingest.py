"""
@author Renata Ann Zeitler
@author Josh Good
@author Jeremy Schmidt
@author Nathaniel Brooks

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

from conans import tools
from pyunpack import Archive

import utils

# Database connection path
database = os.path.realpath(__file__).replace("ingest.py", "duplicates.db")

connection = None  # will remove later, no SQL database
cursor = None      # will remove later, no need for SQL database

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
        scratchDirRoot = os.path.abspath(args.scratch_space)
    else:
        scratchDirRoot = os.path.join(os.path.dirname(os.path.realpath(__file__)), "scratch_space/")

    if not os.path.exists(scratchDirRoot):
        os.makedirs(scratchDirRoot)
    elif not os.path.isdir(scratchDirRoot):
        parser.print_usage()
        print('output_directory is not a directory')
        sys.exit(1)

    if args.output_directory is not None:
        categDirRoot = args.output_directory
    else:
        categDirRoot = os.path.join(os.path.dirname(os.path.realpath(__file__)), "logjam_categories/")


    # Create database in the cwd
    initDatabase(database)

    log_format = "%(asctime)s %(filename)s line %(lineno)d %(levelname)s %(message)s"
    logging.basicConfig(format=log_format, datefmt="%Y-%m-%d %H:%M:%S", level=args.log_level)

    # Ingest the directories
    logging.debug("Ingesting %s", args.ingestion_directory)
    ingest_log_files(args.ingestion_directory, categDirRoot, scratchDirRoot)

    logging.info("Finished")


def ingest_log_files(input_root, output_root, scratch_space):
    for dirs in os.listdir(input_root):
        # if change occurs:
        if dirs != ".DS_Store":
            # flatten the directory
            searchAnInspectionDirectory(os.path.join(input_root, dirs), output_root, scratch_space)


"""
Recursively go through directories to find log files. If compressed, then we need
to unzip/unpack them. Possible file types include: .zip, .gzip, .tar, .tgz, and .7z
start : string
    the start of the file path to traverse
depth : string
    the sub-directories and sub-files associated with this directory
"""
def searchAnInspectionDirectory(start, output_root, scratch_space, depth=None, caseNum=None):
    if not depth:
        depth = ""

    assert os.path.isdir(os.path.join(start + depth)), "This is not a directory: "+os.path.join(start + depth)

    # Loop over each file in the current directory
    for fileOrDir in os.listdir(os.path.join(start + depth)):
        # Check for the file type to make sure it's not compressed
        filename, extension = os.path.splitext(fileOrDir)
        # Get the file's path in inspection dir
        inspecDirPath = start + depth + "/" + fileOrDir
        if caseNum == None: caseNum = getCaseNumber(inspecDirPath)
        assert caseNum != "0", "Not a valid case number: "+caseNum
        # Get category
        category = getCategory(inspecDirPath.lower())
        # Check if this file has been previously ingested into our database
        logging.debug("Checking if duplicate: %s", inspecDirPath)
        cursor.execute("SELECT path FROM paths WHERE path=?", (inspecDirPath,))
        result = cursor.fetchone()
        if (result == None):
            if os.path.isfile(inspecDirPath) and (extension in validExtensions or filename in validFiles):
                copyFileToCategoryDirectory(inspecDirPath, fileOrDir, caseNum, output_root)
            elif os.path.isdir(inspecDirPath):
                # Detected a directory, continue
                searchAnInspectionDirectory(start, output_root, scratch_space, depth=os.path.join(depth + "/" + fileOrDir), caseNum=caseNum)
            elif extension in validZips:
                cursor.execute("INSERT INTO paths(path, flag, category) VALUES(?, ?, ?)", (inspecDirPath, 0, category)) 
                connection.commit()
                
                def handle_unzipped_file(path):
                  # TODO: Change to conditional function
                  # TODO: if is_storagegrid(path):
                  (name,ext) = os.path.splitext(path)

                  if ext in validExtensions or os.path.basename(name) in validFiles:
                    moveFileToCategoryDirectory(path, os.path.basename(path), caseNum, output_root)

                  else:
                    utils.delete_file(path)
                    logging.debug("Ignored non-StorageGRID file: %s", path)
                  return
                
                # TODO: Choose unique folder names per Logjam worker instance
                # TODO: new_scratch_dir = new_unique_scratch_folder()
                new_scratch_dir = os.path.join(scratch_space, "tmp")
                os.makedirs(new_scratch_dir)
                utils.recursive_unzip(inspecDirPath, new_scratch_dir, handle_unzipped_file)
                assert os.path.exists(inspecDirPath), "Should still exist"
                assert os.path.exists(new_scratch_dir), "Should still exist"
                utils.delete_directory(new_scratch_dir)
                
                logging.debug("Added compressed archive to DB & ELK: %s", inspecDirPath)
            else:
                # Invalid file, flag as an error in database and continue
                updateToErrorFlag(inspecDirPath)
                logging.debug("Assumming incorrect filetype: %s", inspecDirPath)
        else:
            # Previously ingested, continue
            logging.debug("Already ingested %s", inspecDirPath)

'''
Assumes the file has not already been copied to the category directory.
Logs the file in the "already scanned" database and then copies the file
to the categories directory.
fullPath : string
    full path for the file
filenameAndExtension : string
    filename + extension for the file, possibly already computed before function call
'''
def copyFileToCategoryDirectory(fullPath, filenameAndExtension, caseNum, categDirRoot):
    assert os.path.isfile(fullPath), "This is not a file: "+fullPath
    assert os.path.split(fullPath)[1] == filenameAndExtension, "Computed filename+extension doesn't match '"+filename+"' - '"+fullPath+"'"
    assert os.path.splitext(filenameAndExtension)[1] in validExtensions or os.path.splitext(filenameAndExtension)[0] in validFiles, "Not a valid file: "+filenameAndExtension

    # Log in the database and copy to the appropriate logjam category
    if caseNum == None: caseNum = getCaseNumber(fullPath)
    assert caseNum != None, "Null reference"
    assert caseNum != "0", "Not a valid case number: "+caseNum

    category = getCategory(fullPath.lower())
    assert category != None, "Null reference"

    if not os.path.exists(categDirRoot):
        os.makedirs(categDirRoot)

    category_dir = os.path.join(categDirRoot, category)
    if not os.path.exists(category_dir):
        os.makedirs(category_dir)

    categDirPath = os.path.join(categDirRoot, category, filenameAndExtension)

    try:
        shutil.copy2(fullPath, categDirPath)  # copy from inspection dir -> Logjam file space
    except (IOError) as e:
        logging.critical(str(e))
        raise e

    timestamp = "%.20f" % time.time()
    basename = "-".join([caseNum, filenameAndExtension, timestamp])
    categDirPathWithTimestamp = os.path.join(categDirRoot, category, basename)

    try:
        os.rename(categDirPath, categDirPathWithTimestamp)
    except (OSError, FileExistsError, IsADirectoryError, NotADirectoryError) as e:
        logging.critical("Unable to rename file: %s", e)
        raise e

    logging.debug("Renamed %s/%s to %s", category, filenameAndExtension, categDirPathWithTimestamp)
    cursor.execute("INSERT INTO paths(path, flag, category) VALUES(?, ?, ?)", (fullPath, 0, category))
    connection.commit()
    logging.debug("Adding %s to db and Logstash", fullPath)

    return

'''
Assumes the file has not already been moved to the category directory.
Logs the file in the "already scanned" database and then moves the file
to the categories folder.

'''
def moveFileToCategoryDirectory(fullPath, filenameAndExtension, caseNum, categDirRoot):
    assert os.path.isfile(fullPath), "This is not a file: "+fullPath
    assert os.path.split(fullPath)[1] == filenameAndExtension, "Computed filename+extension doesn't match '"+filename+"' - '"+fullPath+"'"
    assert os.path.splitext(filenameAndExtension)[1] in validExtensions or os.path.splitext(filenameAndExtension)[0] in validFiles, "Not a valid file: "+filenameAndExtension

    # Log in the database and copy to the appropriate logjam category
    if caseNum == None: caseNum = getCaseNumber(fullPath)
    assert caseNum != None, "Null reference"
    assert caseNum != "0", "Not a valid case number: "+caseNum

    category = getCategory(fullPath.lower())
    assert category != None, "Null reference"

    if not os.path.exists(categDirRoot):
        os.makedirs(categDirRoot)

    if not os.path.exists(categDirRoot + "/" + category):
        os.makedirs(categDirRoot + "/" + category)

    categDirPath = categDirRoot + category + "/" + filenameAndExtension

    try:
        shutil.move(fullPath, categDirPath)  # copy from inspection dir -> Logjam file space
    except (IOError) as e:
        logging.critical("Unable to move file: %s", e)
        raise e

    timestamp = "%.20f" % time.time()
    categDirPathWithTimestamp = categDirRoot + category + "/" + caseNum + "-" + filenameAndExtension + "-" + timestamp

    try:
        os.rename(categDirPath, categDirPathWithTimestamp)
    except (OSError, FileExistsError, IsADirectoryError, NotADirectoryError) as e:
        logging.critical("Unable to rename file: %s", e)
        raise e

    logging.debug("Renamed %s/%s to %s", category, filenameAndExtension, categDirPathWithTimestamp)
    cursor.execute("INSERT INTO paths(path, flag, category) VALUES(?, ?, ?)", (fullPath, 0, category))
    connection.commit()
    logging.debug("Adding %s to db and Logstash", fullPath)

    return

"""
Updates a previously logged entry to have an error flag
path : string
    the file path to update in the database
"""
def updateToErrorFlag(path):
    cursor.execute(''' UPDATE paths SET flag = ? WHERE path = ?''', (1, path,))
    connection.commit()
    logging.debug("Flagging " + path)

"""
Gets the category for this file based on path
path : string
    the path for which to get a category
filename : string
    the file's name
"""
def getCategory(path):
    # Split the path by sub-directories
    splitPath = path.split("/")
    start = splitPath[len(splitPath) - 1]
    splitPath.pop()
    # For each part in this path, run each category regex expression
    # and return the first match
    for part in reversed(splitPath):
        for cat, regex in categories.items():
            if re.search(regex, start):
                return cat
        start = part + "/" + start

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

'''
Creates and initializes database for storing filepaths to prevent duplicates
'''
def initDatabase(db_file):
    global connection
    global cursor

    sql_table = """ CREATE TABLE IF NOT EXISTS paths (
                           path text,
                           flag integer,
                           category text,
                           timestamp float
                        ); """

    try:
        connection = sqlite3.connect(db_file)
        cursor = connection.cursor()
        cursor.execute(sql_table)
    except Error as e:
        logging.critical(str(e))
        raise e

if __name__ == "__main__":
    main()
