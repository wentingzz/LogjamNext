"""
@author Renata Ann Zeitler
@author Josh Good
@author Jeremy Schmidt
@author Nathaniel Brooks
@author Wenting Zheng

Functionality for extracting StorageGRID log fields (platform, version, etc.)
"""


import re
import os


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


def get_category(path):
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


def get_case_number(dir_name):
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


def get_storage_grid_version(path):
    """
    Gets the version of the node from specified file
    path: string
        the path of the specified file (usually the system_command file)
    return: string
        the version if found. Otherwise, returns 'unknown'
    """
    try:
        searchfile = open(path, "r")
        for line in searchfile:
            if "storage-grid-release-" in line:
                searchfile.close()
                return line[21: -1]
        searchfile.close()
        return 'unknown'
    except:
        return 'unknown'


# TODO: implementation
def get_platform(path):
    return 'unknown'

