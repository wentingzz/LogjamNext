"""
@author Renata Ann Zeitler
@author Josh Good

@author Jeremy Schmidt - Updated to python3 09-Sep-2019
@author Nathaniel Brooks - 2019-09-08 No zip inspection dir & treat as read-only

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


# Import packages
import os
import stat
import argparse
import sys
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
cursor = None                       # assign inside main, will remove later no SQL database
connection = None                   # assign inside main, will remove later no SQL database

# Category directory root path to add new files to Logstash
categDirRoot = os.path.realpath(__file__).replace("ingest.py", "logjam_categories/")
# Scratch space directory root path to unzip compressed files to
scratchDirRoot = os.path.realpath(__file__).replace("ingest.py", "scratch_space/")
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

# Globals extracted from main function
verboseprint = lambda *a: None      # do-nothing function


'''
Recursively walks the directories of the inspection
directory, copying relevant files into Logjam controlled
filespace for further processing by Logstash. Unzips compressed
files into Logjam controlled scratchspace, then moves relevant files
for further processing by Logstash.
'''
def main():
    parser = argparse.ArgumentParser(description='File ingestion frontend for Logjam.Next')
    parser.add_argument('-v', '--verbose', dest='is_verbose', action='store_true',
        help='Perform verbose debug printing')
    parser.add_argument(dest='ingestion_directory', action='store',
        help='Directory to ingest files from')
    parser.add_argument('-o', '--output-dir', dest='output_directory', action='store',
        help='Directory to output StorageGRID files to')
    parser.add_argument('-s', '-scratch-space-dir', dest='scratch_space', action='store',
        help='Scratch space directory to unzip files into')
    args = parser.parse_args()
    
    if args.is_verbose:
        def realverboseprint(*args):
            # print arguments separately as to avoid a single long string
            for arg in args:
               print(arg, end=' ')
            print()
        global verboseprint             # set logging if verbose flag, need to change
        verboseprint = realverboseprint
    
    if not os.path.isdir(args.ingestion_directory):
        parser.print_usage()
        print('ingestion_directory is not a directory')
        sys.exit(1)
    
    if args.scratch_space is not None:
        global scratchDirRoot
        scratchDirRoot = args.scratch_space
        
    if not os.path.exists(scratchDirRoot):
        os.makedirs(scratchDirRoot)
        
    if not os.path.isdir(scratchDirRoot):
        parser.print_usage()
        print('output_directory is not a directory')
        sys.exit(1)
    
    if args.output_directory is not None:
        global categDirRoot
        categDirRoot = args.output_directory

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
            searchAnInspectionDirectory(argv[1] + "/" + dirs)

    verboseprint("Finished")

"""
Recursively go through directories to find log files. If compressed, then we need
to unzip/unpack them. Possible file types include: .zip, .gzip, .tar, .tgz, and .7z
start : string
    the start of the file path to traverse
depth : string
    the sub-directories and sub-files associated with this directory
"""
def searchAnInspectionDirectory(start, depth=None, caseNum=None):
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
        category = getCategory(inspecDirPath.lower(), fileOrDir.lower())
        # Check if this file has been previously ingested into our database
        verboseprint("Checking if duplicate:", inspecDirPath)
        cursor.execute("SELECT path FROM paths WHERE path=?", (inspecDirPath,))     
        result = cursor.fetchone()
        if (result == None):
            if os.path.isfile(inspecDirPath) and (extension in validExtensions or filename in validFiles):
                copyFileToCategoryDirectory(inspecDirPath, fileOrDir, caseNum)
            elif os.path.isdir(inspecDirPath):
                # Detected a directory, continue
                verboseprint("This is a directory")                                  
                searchAnInspectionDirectory(start, os.path.join(depth + "/" + fileOrDir), caseNum)
            elif extension in validZips:
                # Zip file, extract contents and parse them
                cursor.execute("INSERT INTO paths(path, flag, category) VALUES(?, ?, ?)", (inspecDirPath, 0, category)) 
                connection.commit()
                unzipIntoScratchSpace(inspecDirPath, extension, caseNum)
                verboseprint("Adding ", inspecDirPath, " to db and Logstash")
            else:
                # Invalid file, flag as an error in database and continue
                updateToErrorFlag(inspecDirPath)
                verboseprint("Assumming incorrect filetype: ", inspecDirPath)  
        else:
            # Previously ingested, continue
            verboseprint("Already ingested", inspecDirPath)

'''
Assumes the file has not already been copied to the category directory.
Logs the file in the "already scanned" database and then copies the file
to the categories directory.
fullPath : string
    full path for the file
filenameAndExtension : string
    filename + extension for the file, possibly already computed before function call
'''
def copyFileToCategoryDirectory(fullPath, filenameAndExtension, caseNum):
    assert os.path.isfile(fullPath), "This is not a file: "+fullPath
    assert os.path.split(fullPath)[1] == filenameAndExtension, "Computed filename+extension doesn't match '"+filename+"' - '"+fullPath+"'"
    assert os.path.splitext(filenameAndExtension)[1] in validExtensions or os.path.splitext(filenameAndExtension)[0] in validFiles, "Not a valid file: "+filenameAndExtension
    
    # Log in the database and copy to the appropriate logjam category
    if caseNum == None: caseNum = getCaseNumber(fullPath)
    assert caseNum != None, "Null reference"
    assert caseNum != "0", "Not a valid case number: "+caseNum
    
    category = getCategory(fullPath.lower(), filenameAndExtension.lower())
    assert category != None, "Null reference"
    
    categDirPath = categDirRoot + category + "/" + filenameAndExtension

    try:
        shutil.copy2(fullPath, categDirPath)  # copy from inspection dir -> Logjam file space
    except (IOError) as e:
        print("Unable to copy file:", e)
        assert False, "Cannot continue execution"
    
    timestamp = "%.20f" % time.time()
    categDirPathWithTimestamp = categDirRoot + category + "/" + caseNum + "-" + filenameAndExtension + "-" + timestamp
    
    try:
        os.rename(categDirPath, categDirPathWithTimestamp)
    except (OSError, FileExistsError, IsADirectoryError, NotADirectoryError) as e:
        print("Unable to rename file:", e)
        assert False, "Cannot continue execution"
    
    verboseprint("Renamed " + category + "/" + filenameAndExtension + " to " + categDirPathWithTimestamp)
    cursor.execute("INSERT INTO paths(path, flag, category) VALUES(?, ?, ?)", (fullPath, 0, category)) 
    connection.commit()
    verboseprint("Adding ", fullPath, " to db and Logstash")

    return

'''
Assumes the file has not already been moved to the category directory.
Logs the file in the "already scanned" database and then moves the file
to the categories folder.

'''
def moveFileToCategoryDirectory(fullPath, filenameAndExtension, caseNum):
    assert os.path.isfile(fullPath), "This is not a file: "+fullPath
    assert os.path.split(fullPath)[1] == filenameAndExtension, "Computed filename+extension doesn't match '"+filename+"' - '"+fullPath+"'"
    assert os.path.splitext(filenameAndExtension)[1] in validExtensions or os.path.splitext(filenameAndExtension)[0] in validFiles, "Not a valid file: "+filenameAndExtension
    
    # Log in the database and copy to the appropriate logjam category
    if caseNum == None: caseNum = getCaseNumber(fullPath)
    assert caseNum != None, "Null reference"
    assert caseNum != "0", "Not a valid case number: "+caseNum
    
    category = getCategory(fullPath.lower(), filenameAndExtension.lower())
    assert category != None, "Null reference"
    
    categDirPath = categDirRoot + category + "/" + filenameAndExtension
    
    try:
        shutil.move(fullPath, categDirPath)  # copy from inspection dir -> Logjam file space
    except (IOError) as e:
        print("Unable to move file:", e)
        assert False, "Cannot continue execution"
    
    timestamp = "%.20f" % time.time()
    categDirPathWithTimestamp = categDirRoot + category + "/" + caseNum + "-" + filenameAndExtension + "-" + timestamp
    
    try:
        os.rename(categDirPath, categDirPathWithTimestamp)
    except (OSError, FileExistsError, IsADirectoryError, NotADirectoryError) as e:
        print("Unable to rename file:", e)
        assert False, "Cannot continue execution"
    
    verboseprint("Renamed " + category + "/" + filenameAndExtension + " to " + categDirPathWithTimestamp)
    cursor.execute("INSERT INTO paths(path, flag, category) VALUES(?, ?, ?)", (fullPath, 0, category)) 
    connection.commit()
    verboseprint("Adding ", fullPath, " to db and Logstash")

    return

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

"""
Unzips the given file path based on extension
path : string
    the path to unzip
extension : string
    the file's extension used in determining the unpacking tool
"""
def unzipIntoScratchSpace(path, extension, caseNum):
    assert os.path.exists(path), "Not a valid path: "+path
    assert os.path.isfile(path), "Should be file: "+path
    assert extension in validZips, "This is not a valid extension: "+extension
    
    # .zip, .tar, and .tgz files
    if extension == ".zip" or extension == ".tar" or extension == ".tgz": 
        verboseprint("Unzipping:", path)
        destPath = path.replace(extension, "")
        (head, destPath) = os.path.split(destPath)
        destPath = scratchDirRoot + destPath
        
        try:                                    # exception handling here only
            tools.unzip(path, destPath, keep_permissions=False)
        except Exception as e:
            print("Error during Conan unzip:", e)
            sys.exit(1)                         # expand as exceptions are discovered
        
        searchAnInspectionDirectory(destPath, "", caseNum)  # search new directory
        
        deleteDirectory(destPath)               # clean up
        
    # .gz files
    elif extension == ".gz":
        verboseprint("Decompressing:", path)
        destPath = path.replace(extension, "")
        (head, destPath) = os.path.split(destPath)
        destPath = scratchDirRoot + destPath
        
        try:                                    # exception handling here only
            decompressedFileData = gzip.GzipFile(path, 'rb').read()
            open(destPath, 'wb').write(decompressedFileData)
        except Exception as e:
            print("Error during GZip unzip:", e)
            sys.exit(1)                         # expand as exceptions are discovered
        
        filename, extension = os.path.splitext(destPath)
        if extension == ".log" or extension == ".txt":  # valid log file
            moveFileToCategoryDirectory(destPath, os.path.split(destPath)[1], caseNum)
        
        elif extension == ".tar":                   # tar file, unpack it
            unzipIntoScratchSpace(destPath, extension, caseNum)
            deleteFile(destPath)                    # remove since not moved
        
        else:                                   # can't handl file type (TODO: Fix this)
            deleteFile(destPath)
    
    # .7z files
    elif extension == ".7z":
        verboseprint("7z Decompressing:", path)
        destPath = path[:-3]
        (head, destPath) = os.path.split(destPath)
        destPath = scratchDirRoot + destPath
        
        # make a directory to unpack the file contents to
        if not os.path.exists(destPath):
            os.makedirs(destPath)
        
        try:                                    # exception handling here only
            Archive(path).extractall(destPath)
        except Exception as e:
            print("Error during pyunpack extraction:", e)
            sys.exit(1)                         # expand as exceptions are discovered
        
        searchAnInspectionDirectory(destPath, "", caseNum)  # parse newly unpacked folder
        
        deleteDirectory(destPath)               # clean up
    
    else:                                       # impossible execution path
        print("This execution path should never be reached")
        sys.exit(1)
    
    return

'''
Attempts to delete a file owned by Logjam, if there is a problem halt the program
fullPath : string
    full path for the file to delete
'''
def deleteFile(fullPath):
    assert os.path.exists(fullPath), "Path does not exist: "+fullPath
    assert os.path.isfile(fullPath), "Path is not a file: "+fullPath
    
    try:
        os.remove(fullPath)
    except Exception as e:
        print("Problem deleting file:", e)
        sys.exit(1)
    
    return

'''
Attempts to recursively delete a directory owned by Logjam, if there is a problem
halt the program
fullPath : string
    full path for the directory to delete
'''
def deleteDirectory(fullPath):
    assert os.path.exists(fullPath), "Path does not exist: "+fullPath
    assert os.path.isdir(fullPath), "Path is not a directory: "+fullPath
    
    try:
        shutil.rmtree(fullPath,onerror=handleDirRemovalErrors)
    except Exception as e:
        print("Problem deleting unzipped folder:", e)
        sys.exit(1)
    
    return
    
'''
Handles errors thrown by shutil.rmtree when trying to remove a
directory that has read-only files on Microsoft Windows.
This elegant solution was originally found on:
https://stackoverflow.com/questions/1889597/deleting-directory-in-python
'''
def handleDirRemovalErrors(func, path, excinfo):
    (t,exc,traceback) = excinfo
    if isinstance(exc, OSError) and exc.errno == 13:
        print("Error deleting {}. Attempting to fix permissions".format(path))
        os.system("chmod -R 755 {}".format(scratchDirRoot))
        func(path)                          # try removing file again
    else:
        print("Unknown exception occured during directory removal")
        print(excinfo)
        print(exc)
        sys.exit(1)

if __name__ == "__main__":
    main()
