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
    is 1970, 00:00:00 (UTC). Designed to be immutable.
    """
    
    def __init__(self, start, stop):
        """ Construct TimePeriod with designated start & stop time.
        """
        assert start < stop, "Start time must be before stop time"
        
        self._start = int(start)
        self._stop = int(stop)
    
    def __eq__(self, other):
        """ Returns whether two TimePeriods are equal.
        """
        if not isinstance(other, TimePeriod):
            return NotImplemented
        
        return self._start == other._start and self._stop == other._stop
    
    def __str__(self):
        """ Returns a readable string representation of this object.
        """
        return ' '.join([str(self._start), str(self._stop)])
    
    def __contains__(self, new_time):
        """ Checks if new time is in the period [start, stop).
        """
        new_time = int(new_time)
        return self._start <= new_time and new_time < self._stop
    
    @property
    def start(self):
        """ Getter property for start
        """
        return self._start
    
    @property
    def stop(self):
        """ Getter property for stop
        """
        return self._stop
    
    @classmethod
    def ancient_history(cls):
        """ Returns a timestamp in ancient history, which for the purposes of
        this program is the date 1900-01-01
        """
        seconds_in_minute = 60
        seconds_in_hour = 60 * seconds_in_minute
        seconds_in_day = 24 * seconds_in_hour
        seconds_in_year = 365 * seconds_in_day
        return 0 - 70 * seconds_in_year             # 70 years before 'epoch' (Jan 1900)


class ScanRecord:
    """ Simple record for storing the scan's time period (start + stop), input
    directory, and last searched path. Designed to be immutable.
    """
    
    def __init__(self, line):
        assert len(line) > 0, "Should not be an empty string"
    
        space_tokens = line.split(' ')
        assert len(space_tokens) >= 4, "Should be at least 4 separate tokens"
        
        start = int(space_tokens[0])
        stop = int(space_tokens[1])
        self.time_period = TimePeriod(start, stop)
        
        quote_tokens = line.split('"')
        assert len(quote_tokens) == 5, "Should be exactly 5 tokens upon split"
        
        self.input_dir = quote_tokens[1]
        assert len(self.input_dir) > 0, "Input directory should be given"
        
        self.last_path = quote_tokens[3]
    
    def __str__(self):
        time = str(self.time_period)
        input = '"' + str(self.input_dir) + '"'
        last = '"' + str(self.last_path) + '"'
        return ' '.join([time, input, last])
    
    def is_complete(self):
        """ Checks to see if this record represents a complete scan. A complete
        scan is denoted by a scan with no last path.
        """
        return len(self.last_path) == 0


class Scan:
    """ Represents an active scan of the input directory.
    """
    
    def __init__(self,input_dir):
        """ Constructs a Scan which operates on the given input directory.
        """
        assert os.path.exists(input_dir), "File path must exist"
        assert os.path.isabs(input_dir), "File path should be absolute"
        
        self.safe_time = int(time.time()) - 6 * 60  # 6 minutes before current time
        self.input_dir = input_dir                  # immutable after construction
        
        self.time_period = TimePeriod(TimePeriod.ancient_history(), self.safe_time)
        self.last_path = None
    
    def update_from_history_file(self, history_file):
        """
        """
        assert os.path.exists(history_file), "History file must exist"
        
        if os.stat(history_file).st_size == 0:      # no previous scans, keep defaults
            return
    
        self.update_from_scan_record(extract_last_scan_record(history_file))
        
    def update_from_scan_record(self, scan_record):
        """ 
        """
        assert scan_record.input_dir == self.input_dir, "Input directories must match"
        
        if scan_record.is_complete():               # completed, new period = then -> safe
            new_start = min(scan_record.time_period.stop, self.safe_time-1)
            new_stop = self.safe_time
            self.time_period = TimePeriod(new_start, new_stop)
            self.last_path = None
        else:                                       # didn't finish, adopt old period
            self.time_period = scan_record.time_period
            self.last_path = scan_record.last_path
        
        assert self.time_period.stop <= self.safe_time, "Must remain within safe time"
        
    def to_scan_record(self):
        """
        """
        return
    
    def should_consider_file(path):
        """ Checks to see if the file denoted by path would be considered for this
        over the given time period.
        """
        assert os.path.isabs(path), "File path should be absolute"
        
        modification_time = os.path.getmtime(path)
        return modification_time in self.time_period


# # # def compute_scan_period(history_file,stop=None):
    # # # """ Computes a new period for scanning based on the last scan. If the last
    # # # scan successfully completed, computes start as the stop of the last scan and stop
    # # # as 6 minutes before the current system time or a caller provided value. If the last
    # # # scan was not successful, justs uses the scanning period of the previous scan.
    # # # """
    # # # assert os.path.isabs(history_file), "File path should be absolute"
    
    # # # start = 0
    
    # # # if os.stat(history_file).st_size != 0:              # history file is not empty
        # # # scan_record = read_last_scan(history_file)
        
        # # # if not scan_record.is_complete():               # last scan didn't finish
            # # # start = scan_record.time_period.start
            # # # stop = scan_record.time_period.stop
        # # # else:                                           # last scan did finish
            # # # start = scan_record.time_period.stop
            # # # if stop == None:
                # # # stop  = int(time.time()) - 60 * 6;      # 6 min before current time
    
    # # # else:                                               # history file was empty
        # # # start = -60 * 60 * 24 * 365 * 30                # 30 years before epoch
        # # # if stop == None:
            # # # stop  = int(time.time()) - 60 * 6;          # 6 min before current time
    
    # # # assert stop <= int(time.time()), "Stop time cannot be in the future"
    # # # return TimePeriod(start, stop)


def extract_last_scan_record(history_file):
    """ Reads the last successful scan information from the scan history
    file denoted by the path and returns the information in a ScanRecord.
    """
    assert os.path.exists(history_file), "File path should exist"
    assert os.path.isabs(history_file), "File path should be absolute"
    assert os.stat(history_file).st_size > 0, "File must contain at least one record"
    
    with open(history_file, "r") as file_stream:
        last_line = file_stream.readlines()[-1]
        return ScanRecord(last_line)


def append_scan_record(history_file, scan_record):
    """ Writes successful scan information to the scan history file denoted by
    the path. The scan time period (start & stop), input directory, and last searched
    path are written as a single line to the file.
    """
    assert os.path.isabs(history_file), "File path should be absolute"
    assert len(scan_record.input_dir) > 0, "Input directory should be given"
    
    with open(history_file, "a") as file:
        file.write(scan_record.to_str()+"\n")
    
    return
