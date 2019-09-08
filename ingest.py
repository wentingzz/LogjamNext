"""
@author Renata Ann Zeitler
@author Josh Good

@author Jeremy Schmidt - Updated to python3 09-Sep-2019
@author Nathaniel Brooks - 2019-09-08 No zip inspection dir & treat as read-only

This script will be used to recursively search through and unzip directories as necessary
and output files with extensions .log and .txt to Logjam

Terminology:
  Inspection Directory - the original directory ingest.py searches through, it should
                         be treated as real-only
  Scratchspace Directory - a directory that ingest.py unzips compressed files into, owned
                           by ingest.py (can R/W there)
  Category Directory - the final directories where ingest.py copies/places files for
                       Logstash to consume, owned by ingest.py (can R/W there)
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
scratchSpaceForUnzipping = os.path.realpath(__file__).replace("ingest.py", "scratch_space/")
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


# Globals extracted from main function
connection = None                   # assign inside main, will remove later no SQL database
cursor = None                       # assign inside main, will remove later no SQL database
verboseprint = lambda *a: None      # do-nothing function


'''
Recursively walks the directories of the inspection
directory, copying relevant files into Logjam controlled
filespace for further processing by Logstash. Unzips compressed
files into Logjam controlled scratchspace, then moves relevant files
for further processing by Logstash.
'''
def main():
    # Starting point, check command line arguments
    if len(argv) != 2 and len(argv) != 3:
        print("\tpython ingest.py [directory to ingest] [-v]")
        exit(1)

    # Check if path is a directory 
    if not os.path.isdir(argv[1]):
        print(argv[1], "is not a directory")
        exit(1)

    # Set logging if the verbose flag was specified
    global verboseprint                    # is a global the best way to do verboseprint?
    if len(argv) == 3 and argv[2] == "-v":
        def realverboseprint(*args):
            # print arguments separately as to avoid a single long string
            for arg in args:
               print(arg, end=' ')
            print()
        verboseprint = realverboseprint
        

    if not os.path.exists(scratchSpaceForUnzipping):
        os.makedirs(scratchSpaceForUnzipping)

    # Establish connection with database and create cursor
    global connection
    connection = sqlite3.connect(database)  # will remove later, no SQL database
    global cursor
    cursor = connection.cursor()        # will remove later, no need for SQL database
    
    # Ingest the directories
    verboseprint("Ingesting ", argv[1])
    for dirs in os.listdir(argv[1]):
        # if change occurs:
        if dirs != ".DS_Store":
            # flatten the directory
            flatten(argv[1] + "/" + dirs)

    verboseprint("Finished")

"""
Recursively go through directories to find log files. If compressed, then we need
to unzip/unpack them. Possible file types include: .zip, .gzip, .tar, .tgz, and .7z
start : string
    the start of the file path to traverse
depth : string
    the sub-directories and sub-files associated with this directory
"""
def searchAnInspectionDirectory(start, depth = None):
    if not depth:
        depth = ""
    # Loop over each file in the current directory
    for fileOrDir in os.listdir(os.path.join(start + depth)):
        # Check for the file type to make sure it's not compressed
        filename, extension = os.path.splitext(fileOrDir)
        # Get the file's path in inspection dir
        inspecDirPath = start + depth + "/" + fileOrDir
        # Get the case number
        caseNum = re.search(r"(\d{10})", inspecDirPath)
        if caseNum is None:
            caseNum = "0"
        else:
            caseNum = caseNum.group()
        # Get category
        category = getCategory(inspecDirPath.lower(), fileOrDir.lower())
        # Check if this file has been previously ingested into our database
        verboseprint("Checking if duplicate:", inspecDirPath)
        cursor.execute("SELECT path FROM paths WHERE path=?", (inspecDirPath,))     
        result = cursor.fetchone()
        if (result == None):
            if os.path.isfile(inspecDirPath) and (extension in validExtensions or filename in validFiles):
                # New file, log in the database and move to the appropriate logjam category
                categoryDirPath = root + category + "/" + fileOrDir
                shutil.copy2(inspecDirPath, categoryDirPath)  # copy from inspection dir -> Logjam file space
                timestamp = "%.20f" % time.time()
                categoryDirPathWithTimestamp = root + category + "/" + caseNum + "-" + fileOrDir + "-" + timestamp
                os.rename(categoryDirPath, categoryDirPathWithTimestamp) 
                verboseprint("Renamed " + category + "/" + fileOrDir + " to " + categoryDirPathWithTimestamp)
                cursor.execute("INSERT INTO paths(path, flag, category) VALUES(?, ?, ?)", (inspecDirPath, 0, category)) 
                connection.commit()
                verboseprint("Adding ", inspecDirPath, " to db and Logstash")
            elif os.path.isdir(inspecDirPath):
                # Detected a directory, continue
                verboseprint("This is a directory")                                  
                searchAnInspectionDirectory(start, os.path.join(depth + "/" + fileOrDir))
            elif extension in validZips:
                # Zip file, extract contents and parse them
                cursor.execute("INSERT INTO paths(path, flag, category) VALUES(?, ?, ?)", (inspecDirPath, 0, category)) 
                connection.commit()
                unzipIntoScratchSpace(inspecDirPath, extension)
                verboseprint("Adding ", inspecDirPath, " to db and Logstash")
            else:
                # Invalid file, flag as an error in database and continue
                updateToErrorFlag(inspecDirPath)
                verboseprint("Assumming incorrect filetype: ", inspecDirPath)  
        else:
            # Previously ingested, continue
            verboseprint("Already ingested", inspecDirPath)


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
def unzipIntoScratchSpace(path, extension):
    verboseprint("Unzipping ", path)
    # Try to unpack the given file using one of the following tools
    try:
        # .zip, .tar, and .tgz files
        if extension == ".zip" or extension == ".tar" or extension == ".tgz": 
            verboseprint("Unzipping:", path)
            unzippedFile = path.replace(extension, "")
            (head, unzippedFile) = os.path.split(unzippedFile)
            unzippedFile = scratchSpaceForUnzipping + unzippedFile
            tools.unzip(path, unzippedFile)
            searchAnInspectionDirectory(unzippedFile)
            # clean up
            shutil.rmtree(unzippedFile)   # okay for now, clean scratch space
        # .gz files
        elif extension == ".gz":
            verboseprint("Decompressing:", path)
            inF = gzip.GzipFile(path, 'rb').read()
            decompressedFile = path.replace(extension, "")
            (head, decompressedFile) = os.path.split(decompressedFile)
            decompressedFile = scratchSpaceForUnzipping + decompressedFile
            file(decompressedFile, 'wb').write(inF)
            filename, extension = os.path.splitext(decompressedFile)
            # valid log file, move it to the current path for ingesting 
            if extension == ".log" or extension == ".txt":
                shutil.move(decompressedFile, path)   # this is bad, moves into inspec dir
            elif extension == ".tar":
                # tar file, unpack it
                unzipIntoScratchSpace(decompressedFile, extension)
            # clean up
            os.remove(decompressedFile)   # okay for now, clean scratch space
        # .7z files
        elif extension == ".7z":
            filePath = path[:-3]
            (head, filePath) = os.path.split(filePath)
            filePath = scratchSpaceForUnzipping + filePath
            # make a directory to unpack the file contents to
            if not os.path.exists(filePath):
                os.makedirs(filePath)
            Archive(path).extractall(filePath)
            # parse the newly unpacked directory and clean up
            searchAnInspectionDirectory(filePath)
            shutil.rmtree(filePath)       # okay for now, clean scratch space
        else :
            # improper file, flag in the database
            verboseprint("Assuming improperly formatted: ", path, "\n")
            updateToErrorFlag(path)
    except Exception as e:
        # encountered an error, flag in the database
        verboseprint("Error: could not unzip ", path)
        updateToErrorFlag(path)

if __name__ == "__main__":
    main()
