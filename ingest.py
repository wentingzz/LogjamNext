"""
@author Renata Ann Zeitler
@author Josh Good

@author Jeremy Schmidt - Updated to python3 09-Sep-2019

This script will be used to recursively search through and unzip directories as necessary and
output files with extensions .log and .txt to Logjam
"""
# Import packages
import os
import argparse
from sys import argv
# Tools for unzipping files
import gzip
from conans import tools
from pyunpack import Archive, PatoolError
import string
# database
import sqlite3
import time
import shutil
import re
# Database connection path
database = os.path.realpath(__file__).replace("ingest.py", "duplicates.db")
cursor = None
connection = None

# root directory to add new files to Logstash
root = os.path.realpath(__file__).replace("ingest.py", "logjam_categories/")
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

"""
Recursively go through directories to find log files. If compressed, then we need to unzip/unpack them. Possible
file types include: .zip, .gzip, .tar, .tgz, and .7z
start : string
    the start of the file path to traverse
depth : string
    the sub-directories and sub-files associated with this directory
"""
def flatten(start, depth = None):
    if not depth:
        depth = ""
    # Loop over each file in the current directory
    for fileOrDir in os.listdir(os.path.join(start + depth)):
        # Check for the file type to make sure it's not compressed
        filename, extension = os.path.splitext(fileOrDir)
        # Get the file's path
        path = start + depth + "/" + fileOrDir
        # Get the case number
        caseNum = re.search(r"(\d{10})", path)
        if caseNum is None:
            caseNum = "0"
        else:
            caseNum = caseNum.group()
        # Get category
        category = getCategory(path.lower(), fileOrDir.lower())
        # Check if this file has been previously ingested into our database
        verboseprint("Checking if duplicate:", path)
        cursor.execute("SELECT path FROM paths WHERE path=?", (path,))     
        result = cursor.fetchone()
        if (result == None):
            if os.path.isfile(path) and (extension in validExtensions or filename in validFiles):
                # New file, log in the database and move to the appropriate logjam category
                shutil.move(path, root + category + "/" + fileOrDir)
                timestamp = "%.20f" % time.time()
                os.rename(root + category + "/" + fileOrDir, root + category + "/" + caseNum + "-" + fileOrDir + "-" + timestamp) 
                verboseprint("Renamed " + category + "/" + fileOrDir + " to " + root + category + "/" + caseNum + "-" + fileOrDir + "-" + timestamp)
                cursor.execute("INSERT INTO paths(path, flag, category) VALUES(?, ?, ?)", (path, 0, category)) 
                connection.commit()
                verboseprint("Adding ", path, " to db and Logstash")
            elif os.path.isdir(path):
                # Detected a directory, continue
                verboseprint("This is a directory")                                  
                flatten(start, os.path.join(depth + "/" + fileOrDir))
            elif extension in validZips:
                # Zip file, extract contents and parse them
                cursor.execute("INSERT INTO paths(path, flag, category) VALUES(?, ?, ?)", (path, 0, category)) 
                connection.commit()
                unzip(path, extension)
                verboseprint("Adding ", path, " to db and Logstash")
            else:
                # Invalid file, flag as an error in database and continue
                updateToErrorFlag(path)
                verboseprint("Assumming incorrect filetype: ", path)  
        else:
            # Previously ingested, continue
            verboseprint("Already ingested", path)


"""
Updates a previously logged entry to have an error flag
path : string
    the file path to update in the database
"""
def updateToErrorFlag(path):
    cursor.execute(''' UPDATE paths SET flag = ? WHERE path = ?''', (1, path,))
    connection.commit()
    verboseprint("Flagging " + path)

"""
Gets the category for this file based on path
path : string
    the path for which to get a category
filename : string
    the file's name
"""
def getCategory(path, filename):
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

"""
Unzips the given file path based on extension
path : string
    the path to unzip
extension : string
    the file's extension used in determining the unpacking tool
"""
def unzip(path, extension):
    verboseprint("Unzipping ", path)
    # Try to unpack the given file using one of the following tools
    try:
        # .zip, .tar, and .tgz files
        if extension == ".zip" or extension == ".tar" or extension == ".tgz": 
            verboseprint("Unzipping:", path)
            unzippedFile = path.replace(extension, "")
            tools.unzip(path, unzippedFile)
            flatten(unzippedFile)
            # clean up
            shutil.rmtree(unzippedFile)
        # .gz files
        elif extension == ".gz":
            verboseprint("Decompressing:", path)
            inF = gzip.GzipFile(path, 'rb').read()
            decompressedFile = path.replace(extension, "")
            file(decompressedFile, 'wb').write(inF)
            filename, extension = os.path.splitext(decompressedFile)
            # valid log file, move it to the current path for ingesting 
            if extension == ".log" or extension == ".txt":
                shutil.move(decompressedFile, path)
            elif extension == ".tar":
                # tar file, unpack it
                unzip(decompressedFile, extension)
            # clean up
            os.remove(decompressedFile)
        # .7z files
        elif extension == ".7z":
            filePath = path[:-3]
            # make a directory to unpack the file contents to
            if not os.path.exists(filePath):
                os.makedirs(filePath)
            Archive(path).extractall(filePath)
            # parse the newly unpacked directory and clean up
            flatten(filePath)
            shutil.rmtree(filePath)
        else :
            # improper file, flag in the database
            verboseprint("Assuming improperly formatted: ", path, "\n")
            updateToErrorFlag(path)
    except Exception as e:
        # encountered an error, flag in the database
        verboseprint("Error: could not unzip ", path)
        updateToErrorFlag(path)


# Starting point, check command line arguments
if len(argv) != 2 and len(argv) != 3:
    print("\tpython ingest.py [directory to ingest] [-v]")
    exit(1)

# Check if path is a directory 
if not os.path.isdir(argv[1]):
    print(argv[1], "is not a directory")
    exit(1)

# Set logging if the verbose flag was specified
if len(argv) == 3 and argv[2] == "-v":
    def verboseprint(*args):
        # print arguments separately as to avoid a single long string
        for arg in args:
           print(arg, end=' ')
        print()
else:   
    verboseprint = lambda *a: None      # do-nothing function

# Establish connection with database and create cursor
connection = sqlite3.connect(database)
cursor = connection.cursor()


# Ingest the directories
verboseprint("Ingesting ", argv[1])
for dirs in os.listdir(argv[1]):
    # if change occurs:
    if dirs != ".DS_Store":
        # flatten the directory
        flatten(argv[1] + "/" + dirs)

verboseprint("Finished")
