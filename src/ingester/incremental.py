"""
@author Nathaniel Brooks
Utility file for incremental scanning.
"""


import os


class TimePeriod:
    """ Represents a period in time defined by a start and stop time. These start
    and stop times are measured in integer seconds since the 'epoch'. On Unix this
    is 1970, 00:00:00 (UTC).
    """
    
    def __init__(self, start, stop):
        """ Construct TimePeriod with designated start & stop time.
        """
        self.start = int(start)
        self.stop = int(stop)
    
    def contains(self, new_time):
        """ Checks to see if the new time is within this time period.
        """
        new_time = int(new_time)
        return start <= new_time and new_time <= stop


class ScanHistoryRecord:
    """ Simple record for storing the scan's time period (start + stop), input
    directory, and last searched path.
    """
    
    def __init__(self):
        """ Construct ScanHistoryRecord with empty fields.
        """
        self.time_period = None
        self.input_directory = None
        self.last_path = None


def file_within_scan(path, time_period):
    """ Checks to see if the file denoted by path would be considered for a scan
    that spans the given time period.
    """
    assert os.path.isabs(path), "File path should be absolute"
    
    modification_time = os.path.getmtime(path)
    return time_period.contains(modification_time)


def read_last_scan(path):
    """ Reads the last successful scan information from the scan history
    file denoted by path and returns the information in a ScanHistoryRecord.
    """
    assert os.path.isabs(path), "File path should be absolute"
    
    with open(path, "r") as file:
        last_line = file.readlines()[-1]
        
        tokens = last_line.split()
        start = int(tokens[0])
        stop = int(tokens[1])
        input_dir = tokens[1:-2]
        last_path = tokens[1:-2]
        
        return ScanHistoryRecord(TimePeriod(start,stop),input_dir,last_path)


def write_last_scan(path, scan_record):
    """ Writes successful scan information to the scan history file denoted by
    path. The scan time period (start & stop), input directory, and last searched
    path are written as a single line to the file.
    """
    assert os.path.isabs(path), "File path should be absolute"
    
    with open(path, "a") as file:
        s = str(scan_record.time_period.start) +
            str(scan_record.time_period.stop) +
            '"' + scan_record.input_directory + '"' +
            '"' + scan_record.last_path + '"'
            
        file.write(s+"\n")
    
    return
