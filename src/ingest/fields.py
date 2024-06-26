"""
@author Renata Ann Zeitler
@author Josh Good
@author Jeremy Schmidt
@author Nathaniel Brooks
@author Wenting Zheng
@author Daniel Grist

Functionality for extracting StorageGRID log fields (platform, version, etc.)
and handling other StorageGRID related tasks such as identifying valid StorageGRID logs.
"""


import re
import os
import logging
import paths


MISSING_CASE_NUM = "Unknown"
MISSING_SG_VER = (-1, -1)
MISSING_PLATFORM = "Unknown"
MISSING_CATEGORY = "other"
MISSING_TIME_SPAN = "Unknown-Unknown"
MISSING_NODE_NAME = "Unknown"
MISSING_GRID_ID = "Unknown"

HV_ENV_TO_PLATFORM = {                              
    "vSphere" : "vSphere",                          
    "SGA" : "SGA",                                  
}

# List of all categories to sort log files by
CATEGORIES = {
    "audit" : r".*audit.*", "base_os_commands" : r".*base[/_-]*os[/_-]*.*command.*",
    "bycast" : r".*bycast.*", "cassandra_commands" : r".*cassandra[/_-]*command.*",
    "cassandra_gc" : r".*cassandra[/_-]*gc.*",
    "cassandra_system" : r".*cassandra[/_-]*system.*", "dmesg" : r".*dmesg.*",
    "gdu_server" : r".*gdu[/_-]*server.*", "init_sg": r".*init[/_-]*sg.*", 
    "install": r".*install.*", "kern" : r".*kern.*", "messages": r".*messages.*", 
    "pge_image_updater": r".*pge[/_-]*image[/_-]*updater.*", 
    "pge_mgmt_api" : r".*pge[/_-]*mgmt[/_-]*api.*", "server_manager" : r".*server[/_-]*manager.*",
    "sg_fw_update" : r".*sg[/_-]*fw[/_-]*update.*", "storagegrid_node" : r".*storagegrid.*node.*",
    "storagegrid_daemon" : r".*storagegrid.*daemon.*",
    "syslog":".*syslog.*","system_commands":r".*system[/_-]*commands.*","upgrade":r".*upgrade.*"
}

# Extensions to use outside lumberjacks
VALID_LOG_EXTENSIONS = [                       
    ".txt",
    ".log",
]

# Filenames to use outside lumberjack
VALID_LOG_FILENAMES = [                        
    "syslog",
    "messages",
    "system_commands",
]

class NodeFields:
    """
    Record representing possible fields for a StorageGRID Node. Can be used
    as a simple storage container for the fields or can inherit fields
    from another NodeFields object.
    """
    
    @classmethod
    def from_lumberjack_dir(cls, lumber_dir):
        """
        Builds a NodeFields object by extracting relevant fields from the
        specified lumberjack directory (a directory with a lumberjack.log file).
        """
        assert os.path.isfile(os.path.join(lumber_dir, "lumberjack.log"))
        
        sg_ver = get_storage_grid_version(lumber_dir)
        platform = get_platform(lumber_dir)
        category = get_category(lumber_dir)
        time_span = get_time_span(lumber_dir)
        node_name = get_node_name(lumber_dir)
        grid_id = get_grid_id(lumber_dir)
        
        return NodeFields(  sg_ver=sg_ver,          
                            platform=platform,
                            category=category,
                            time_span=time_span,
                            node_name=node_name,
                            grid_id=grid_id)
    
    def __init__(   self, *,
                    case_num=MISSING_CASE_NUM,
                    sg_ver=MISSING_SG_VER,
                    platform=MISSING_PLATFORM,
                    category=MISSING_CATEGORY,
                    time_span=MISSING_TIME_SPAN,
                    node_name=MISSING_NODE_NAME,
                    grid_id=MISSING_GRID_ID):
        """ Constructs basic structure with fields, named params are forced """
        self._case_num = case_num
        self._sg_ver = sg_ver
        self._platform = platform
        self._category = category
        self._time_span = time_span
        self._node_name = node_name
        self._grid_id = grid_id
    
    def inherit_missing_from(self, other):
        """ Inherits missing values from the other NodeFields object """
        self._case_num = self._case_num if self._case_num != MISSING_CASE_NUM else other._case_num
        self._sg_ver = self._sg_ver if self._sg_ver != MISSING_SG_VER else other._sg_ver
        self._platform = self._platform if self._platform != MISSING_PLATFORM else other._platform
        self._category = self._category if self._category != MISSING_CATEGORY else other._category
        self._time_span = self._time_span if self._time_span != MISSING_TIME_SPAN else other._time_span
        self._node_name = self._node_name if self._node_name != MISSING_NODE_NAME else other._node_name
        self._grid_id = self._grid_id if self._grid_id != MISSING_GRID_ID else other._grid_id
        
    @property
    def case_num(self):
        """ Getter property for case_num """
        return self._case_num
    
    @property
    def sg_ver(self):
        """ Getter property for sg_ver """
        return self._sg_ver
    
    @property
    def platform(self):
        """ Getter property for platform """
        return self._platform
    
    @property
    def category(self):
        """ Getter property for category """
        return self._category
    
    @property
    def time_span(self):
        """ Getter property for time_span """
        return self._time_span
    
    @property
    def node_name(self):
        """ Getter property for node_name """
        return self._node_name
    
    @property
    def grid_id(self):
        """ Getter property for grid_id """
        return self._grid_id


def get_category(lumber_dir):
    """
    Gets the category for a StorageGRID Node based on the lumberjack directory provided
    lumber_dir : string
        the lumberjack directory to search for category
    return : string
        the category of the directory or MISSING_CATEGORY if not found
    """
    # Split the path by sub-directories
    splitPath = lumber_dir.replace('\\','/').split("/")
    start = splitPath[len(splitPath) - 1]
    splitPath.pop()
    # For each part in this path, run each category regex expression
    # and return the first match
    for part in reversed(splitPath):
        for cat, regex in CATEGORIES.items():
            if re.search(regex, start):
                return cat
        start = os.path.join(part, start)

    # Unrecognized file, so return "other"
    return MISSING_CATEGORY


def get_case_number(case_dir):
    """
    Extracts the tech support case number from a directory name.
    case_dir : string
        path to the directory to search for case number
    return : string
        the case number that was found or MISSING_CASE_NUM is nothing was found
    """
    match_obj = re.match(r"^(\d{10})$", os.path.basename(case_dir))
    if match_obj is None:
        return MISSING_CASE_NUM
    else:
        return match_obj.group()


def get_storage_grid_version(lumber_dir):
    """
    Gets the version of the node from the specified lumberjack directory.
    Example: if a line has storage-grid-release-10.4.100-12345678.0224,
    The major and major version would be 10 and the minor version is 4

    lumber_dir: string
        the path of the specified lumberjack directory
    return: tuple of major version and minor version if
        the version if found, otherwise MISSING SG_VER
    """
    SG_RELEASE = "storage-grid-release-"
    sys_file = os.path.join(lumber_dir, "system_commands")

    # use system_commands file to find version
    if os.path.isfile(sys_file):
        try:
            with open(sys_file, "r") as file:
                # read system_commands line by line
                for line in file:
                    if SG_RELEASE in line:
                        _, version = line.split(SG_RELEASE)
                        major, minor, *_ = version.split(".")
                        # return a tuple of major and minor version
                        return (int(major), int(minor))
        except Exception as e:
            logging.warning("Error while parsing storagegrid version: %s", str(e))

    return MISSING_SG_VER


def get_platform(lumber_dir):
    """
    Gets the platform of the node from the specified lumberjack directory.
    lumber_dir: string
        the path of the specified lumberjack directory
    return: string
        the platform if found, otherwise MISSING_PLATFORM
    """
    user_data_file = paths.QuantumEntry(lumber_dir, "os/etc/user_data")
    if user_data_file.is_file():
        with open(user_data_file.abspath, "r") as fd:
            for line in fd:
                # Take part after "=" and remove other punctuation characters
                if "HV_ENV" in line and "=" in line:
                    val = line.split("=")[1]        
                    val = val.strip("\n\"\'; ")     
                    if val in HV_ENV_TO_PLATFORM:   
                        return HV_ENV_TO_PLATFORM[val]
                    else:
                        logging.warning("Unknown Platform: %s", val)
    
    return MISSING_PLATFORM


def get_time_span(lumber_dir):
    """
    Gets the time span represented by the given lumberjack directory.
    The time span format is two numbers with a dash between them (ex. 0000-0000)
    lumber_dir: string
        the path of the specified lumberjack directory
    return: string
        the time span with a dash in between, otherwise MISSING_TIME_SPAN
    """
    match_obj = re.match(r"^([\d]+[-][\d]+)$", os.path.basename(lumber_dir))
    if match_obj is None:
        return MISSING_TIME_SPAN
    else:
        return match_obj.group()


def get_node_name(lumber_dir):
    """
    Gets the node name represented by the given lumberjack directory.
    The node name is located two directories above the lumberjack directory.
    lumber_dir: string
        the path of the specified lumberjack directory
    return: string
        the name of the node, empty string otherwise
    """
    return os.path.basename(os.path.dirname(lumber_dir))


def get_grid_id(lumber_dir):
    """
    Gets the grid id represented by the given lumberjack directory.
    The grid id is located three directories above the lumberjack directory.
    lumber_dir: string
        the path of the specified lumberjack directory
    return: string
        the grid id, otherwise empty string
    """
    return os.path.basename(os.path.dirname(os.path.dirname(lumber_dir)))


def extract_fields(lumber_dir, *, inherit_from):
    """
    Extracts all relevant StorageGRID fields from the lumberjack directory
    denoted by `lumber_dir` and returns a new NodeFields object with the fields.
    The object inherits missing values from `inherit_from` NodeFields object.
    All files under the `lumber_dir` are valid for extracting fields from.
    lumber_dir: string
        the path of the specified lumberjack directory
    inherit_from: NodeFields object
        object that is used to inherit missing values
    return: Nodefields object
        extracted fields
    """
    new_fields = NodeFields.from_lumberjack_dir(lumber_dir)
    
    new_fields.inherit_missing_from(inherit_from)
    
    return new_fields


def contains_bycast(entry_path):
    """
    Returns true if the entry in question has the text string `bycast` in either
    its path or its contents. If the entry is a directory, only check its path.
    entry_path: string
        the path that is being inspected
    return: bool
        True if the path contains "bycast"
    """
    if "bycast" in entry_path:
        return True
    
    if not os.path.exists(entry_path) or os.path.isdir(entry_path):
        return False

    try:
        with open(entry_path, "r") as searchfile:
            for line in searchfile:
                if "bycast" in line:
                    return True
    except Exception as e:
        logging.warning('Error during "bycast" search: %s', str(e))
    
    return False


def is_storagegrid(nodefields, entry):
    """
    Determines whether the entry is related to StorageGRID. Rejects files without
    a correct extension or filename. If the file is outside a lumberjack directory,
    it additionally performs a full bycast search on the path & contents.
    nodefields: Nodefields object
        Object containing the node fields 
    entry: QuantumEntry
        path for the entry that is being checked
    return: bool
        True if the entry pertains to StorageGRID
    """
    assert isinstance(nodefields, NodeFields), "Wrong argument type"
    assert isinstance(entry, paths.QuantumEntry), "Wrong argument type"
    
    valid_ext = entry.extension in VALID_LOG_EXTENSIONS
    valid_name = entry.filename in VALID_LOG_FILENAMES
    valid_path = valid_ext or valid_name
    
    if not valid_path:
        return False
    
    if nodefields.node_name != MISSING_NODE_NAME:
        return True
    else:                                       
        return valid_path and contains_bycast(entry.abspath)

