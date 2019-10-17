"""
@author Nathaniel Brooks
Utility file for incremental scanning.
"""


import os
import time
from collections import namedtuple


class TimePeriod:
    """ Represents a period in time defined by a start and stop time. These start
    and stop times are measured in integer seconds since the 'epoch'. On Unix this
    is 1970, 00:00:00 (UTC).
    """
    
    def __init__(self, start, stop):
        """ Construct TimePeriod with designated start & stop time.
        """
        assert start < stop, "Start time must be before stop time"
        
        self.start = int(start)
        self.stop = int(stop)
    
    def contains(self, new_time):
        """ Checks to see if the new time is within this time period. Start is
        inclusive & stop is exclusive.
        """
        new_time = int(new_time)
        return start <= new_time and new_time < stop


class Scan:
    """
    """
    
    def __init__(self,time_period=TimePeriod(0,0),input_dir="",last_path=""):
        """ Construct ScanRecord with designated fields.
        """
        self.time_period = time_period
        self.input_directory = input_dir
        self.last_path = last_path
    
    def is_complete(self):
        """ Checks to see if this record represents a complete scan. A complete
        scan is denoted by a scan with no last path.
        """
        assert isintance(self.last_path, str), "Last path should be a string"
        assert len(self.input_dir) > 0, "Input directory should be given"
        
        return len(last_path) == 0


ScanRecord = namedtuple("ScanRecord", ["time_period","input_directory","last_path"])
""" Simple record for storing the scan's time period (start + stop), input
directory, and last searched path.
"""


def compute_scan_period(history_file,stop_time=None):
    """ Computes a new period for scanning based on the last scan. If the last
    scan successfully completed, computes start as the stop of the last scan and stop
    as 6 minutes before the current system time or a caller provided value. If the last
    scan was not successful, justs uses the scanning period of the previous scan.
    """
    assert os.path.isabs(history_file), "File path should be absolute"
    
    start_time = 0
    
    if os.stat(history_file).st_size != 0:              # history file is not empty
        scan_record = read_last_scan(history_file)
        
        if not scan_record.is_complete():               # last scan didn't finish
            start_time = scan_record.time_period.start
            stop_time = scan_record.time_period.stop
        else:                                           # last scan did finish
            start_time = scan_record.time_period.stop
            if stop_time == None:
                stop_time  = int(time.time()) - 60 * 6; # 6 min before current time
    
    else:                                               # history file was empty
        start_time = -60 * 60 * 24 * 365 * 30           # 30 years before epoch
        if stop_time == None:
            stop_time  = int(time.time()) - 60 * 6;     # 6 min before current time
    
    assert stop_time <= int(time.time()), "Stop time cannot be in the future"
    return TimePeriod(start_time, stop_time)


def file_within_scan(path, time_period):
    """ Checks to see if the file denoted by path would be considered for a scan
    that spans the given time period.
    """
    assert os.path.isabs(path), "File path should be absolute"
    
    modification_time = os.path.getmtime(path)
    return time_period.contains(modification_time)


def read_last_scan(history_file):
    """ Reads the last successful scan information from the scan history
    file denoted by the path and returns the information in a ScanRecord.
    """
    assert os.path.isabs(history_file), "File path should be absolute"
    
    with open(history_file, "r") as file:
        last_line = file.readlines()[-1]
        
        tokens = last_line.split()
        start = int(tokens[0])
        stop = int(tokens[1])
        input_dir = tokens[1:-2]
        last_path = tokens[1:-2]
        
        return ScanRecord(TimePeriod(start,stop),input_dir,last_path)


def write_last_scan(history_file, scan_record):
    """ Writes successful scan information to the scan history file denoted by
    the path. The scan time period (start & stop), input directory, and last searched
    path are written as a single line to the file.
    """
    assert os.path.isabs(history_file), "File path should be absolute"
    assert len(scan_record.input_directory) > 0, "Input directory should be given"
    
    with open(history_file, "a") as file:
        s = str(scan_record.time_period.start) +
            str(scan_record.time_period.stop) +
            '"' + scan_record.input_directory + '"' +
            '"' + scan_record.last_path + '"'
            
        file.write(s+"\n")
    
    return
