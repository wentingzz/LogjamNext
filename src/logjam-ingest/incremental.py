"""
@author Nathaniel Brooks

Utility file for incremental scanning.
"""


import os
import time


seconds_between_automatic_history_updates = 120


class TimePeriod:
    """
    Represents a period in time defined by a start and stop time. These start
    and stop times are measured in integer seconds since the 'epoch'. On Unix this
    is 1970, 00:00:00 (UTC). Designed to be immutable.
    """

    def __init__(self, start, stop):
        """ Construct TimePeriod with designated start & stop time. """
        assert start < stop, "Start time must be before stop time"

        self._start = int(start)
        self._stop = int(stop)

    def __eq__(self, other):
        """ Returns whether two TimePeriods are equal. """
        if not isinstance(other, TimePeriod):
            raise NotImplementedError("Can only compare TimePeriod")

        return self._start == other._start and self._stop == other._stop

    def __str__(self):
        """ Returns a readable string representation of this object. """
        return ' '.join([str(self._start), str(self._stop)])

    def __contains__(self, new_time):
        """ Checks if new time is in the period [start, stop). """
        new_time = int(new_time)
        return self._start <= new_time and new_time < self._stop

    @property
    def start(self):
        """ Getter property for start """
        return self._start

    @property
    def stop(self):
        """ Getter property for stop """
        return self._stop

    @classmethod
    def ancient_history(cls):
        """
        Returns a timestamp in ancient history, which for the purposes of
        this program is the date 1900-01-01
        """
        seconds_in_minute = 60
        seconds_in_hour = 60 * seconds_in_minute
        seconds_in_day = 24 * seconds_in_hour
        seconds_in_year = 365 * seconds_in_day
        return 0 - 70 * seconds_in_year             # 70 years before 'epoch' (Jan 1900)


class ScanRecord:
    """
    Simple record for storing the scan's time period (start + stop), input
    directory, and last searched path. Designed to be immutable.
    """

    @classmethod
    def from_str(cls, line):
        """
        Builds a ScanRecord from a string representation of a ScanRecord.
        This operation is the opposite of the `__str__` method.
        """
        assert isinstance(line, str)
        assert len(line) > 0, "Should not be an empty string"

        space_tokens = line.split(' ')
        assert len(space_tokens) >= 4, "Should be at least 4 separate tokens"

        start = int(space_tokens[0])
        stop = int(space_tokens[1])

        quote_tokens = line.split('"')
        assert len(quote_tokens) == 5, "Should be exactly 5 tokens upon split"

        input_dir = quote_tokens[1]
        assert len(input_dir) > 0, "Input directory should be given"

        last_path = quote_tokens[3]

        return ScanRecord(start, stop, input_dir, last_path)

    def __init__(self, start, stop, input_dir, last_path):
        """ Constructs a ScanRecord by just copying parameters given """
        assert len(input_dir) > 0, "Input directory cannot be empty"
        assert isinstance(last_path, str)

        self._time_period = TimePeriod(start, stop)
        self._input_dir = input_dir
        self._last_path = last_path

    def __eq__(self, other):
        """ Returns whether two ScanRecords are equal. """
        if not isinstance(other, ScanRecord):
            raise NotImplementedError("Can only compare ScanRecord")

        return self._time_period == other._time_period and \
            self._input_dir == other._input_dir and \
            self._last_path == other._last_path

    def __str__(self):
        """
        Returns a string representation of the ScanRecord. Each field is
        separated by a single space.
        """
        time = str(self._time_period)
        input = '"' + str(self._input_dir) + '"'
        last = '"' + str(self._last_path) + '"'
        return ' '.join([time, input, last])

    def is_complete(self):
        """
        Checks to see if this record represents a complete scan. A complete
        scan is denoted by a scan with no last path.
        """
        return len(self._last_path) == 0

    @property
    def time_period(self):
        """ Getter property for time_period """
        return self._time_period

    @property
    def input_dir(self):
        """ Getter property for input_dir """
        return self._input_dir

    @property
    def last_path(self):
        """ Getter property for last_path """
        return self._last_path


class Scan:
    """ Represents an active scan of the input directory. """

    def __init__(self, input_dir, history_dir):
        """ Constructs a Scan which operates on the given input directory. """
        assert os.path.exists(input_dir), "File path must exist"

        self.safe_time = int(time.time()) - 6 * 60  # 6 minutes before current time
        self.input_dir = input_dir                  # these 5 fields immutable after init
        self.history_dir = history_dir
        self.history_active_file = os.path.join(history_dir, "scan-history-active.txt")
        self.history_log_file = os.path.join(history_dir, "scan-history-log.txt")

        self.last_path = ""
        self.last_history_update = TimePeriod.ancient_history()

        self.time_period = TimePeriod(TimePeriod.ancient_history(), self.safe_time)

        if not os.path.exists(self.history_dir):
            pass                                    # no history dir, will make one
        elif not os.path.exists(self.history_active_file):
            pass                                    # no active file, will make one
        elif os.stat(self.history_active_file).st_size == 0:
            pass                                    # no previous scans, keep defaults
        else:
            last_scan = extract_last_scan_record(self.history_active_file)
            self._update_from_scan_record(last_scan)# update from previous scans
        
        os.makedirs(self.history_dir, exist_ok=True)# make sure history dir is ready
        open(self.history_active_file, 'a').close() # make sure active file is ready
        open(self.history_log_file, 'a').close()    # make sure log file is ready

    def _update_from_scan_record(self, scan_record):
        """
        Updates the Scan by inspecting the last ScanRecord. If the ScanRecord
        was not completed, adopt the old ScanRecord's time period. If the ScanRecord
        did complete, new period is from the stop of the last ScanRecord to the
        closest safe time (defined as 6 minutes before the current time, to allow
        for safe updates of the modification time on directories).
        """
        assert not self._is_closed(), "Scan was internally closed"
        assert scan_record.input_dir == self.input_dir, "Input directories must match"

        if scan_record.is_complete():               # completed, new period = then -> safe
            new_start = min(scan_record.time_period.stop, self.safe_time-1)
            new_stop = self.safe_time
            self.time_period = TimePeriod(new_start, new_stop)
            self.last_path = ""
        else:                                       # didn't finish, adopt old period
            self.time_period = scan_record.time_period
            self.last_path = scan_record.last_path

        assert self.time_period.stop <= self.safe_time, "Must remain within safe time"

    def _to_scan_record(self):
        """ Returns a ScanRecord representing this Scan at a moment in time """
        assert not self._is_closed(), "Scan was internally closed"

        return ScanRecord(
            self.time_period.start,
            self.time_period.stop,
            self.input_dir,
            self.last_path)

    def just_scanned_this_path(self, path):
        """
        Caller just scanned the given path, so update the internal last
        scanned path variable and possibly write the file to our history file if
        enough time has passed.
        """
        assert not self._is_closed(), "Scan was internally closed"
        assert os.path.exists(path), "Path should exist on system"

        self.last_path = path

        self._save_state_to_file(force_save=False)

    def should_consider_file(self, path):
        """
        Checks to see if the file denoted by path would be considered for this
        scan over the given time period.
        """
        assert not self._is_closed(), "Scan was internally closed"
        assert os.path.exists(path), "File should exist on system"
        assert not os.path.isdir(path), "Path should point to a file"

        modification_time = os.path.getmtime(path)
        return modification_time in self.time_period

    def complete_scan(self):
        """
        Completes the scan, writing out information to the history files
        to show that the scan was completed.
        """
        assert not self._is_closed(), "Scan was internally closed"

        self.last_path = ""

        self._save_state_to_file(force_save=True)

        self._close()                           # internally close the Scan

    def premature_exit(self):
        """
        Program needs to halt the scan prematurely. Write out information
        to history files so that it can hopefully be picked up next time.
        """
        assert not self._is_closed(), "Scan was internally closed"

        if self.last_path == "":                # no successfully scanned paths so far
            self.input_dir = None               # internally close the Scan
            return                              # don't write to history, nothing scanned

        self._save_state_to_file(force_save=True)

        self._close()                           # internally close the Scan
    
    def _close(self):
        """ Internally closes the Scan by nullifying input_dir """
        assert not self._is_closed(), "Scan was internally closed"
        
        self.input_dir = None                   # nullify always valid input_dir
    
    def _is_closed(self):
        """ Checks to see if the Scan is internally closed """
        return self.input_dir == None           # input_dir always valid, must have closed
    
    def _save_state_to_file(self, *, force_save=False):
        """
        Attempts to save the state of this Scan object to the appropriate
        history files, with the active getting the current state and the log
        getting all the past states. If the parameter force_save is not specified
        then the state is only saved after a certain time limit has passed.
        """
        assert not self._is_closed(), "Scan was internally closed"
        
        cur_time = int(time.time())

        if not force_save and cur_time-self.last_history_update <= seconds_between_automatic_history_updates:
            return
        
        new_record = self._to_scan_record()
        assert new_record.is_complete() == (self.last_path == ""), "Bad completion status"
        
        append_scan_record(self.history_log_file, new_record)
        overwrite_scan_record(self.history_active_file, new_record)
        self.last_history_update = cur_time


def extract_last_scan_record(path):
    """
    Reads the last successful scan information from the scan history
    file denoted by the path and returns the information in a ScanRecord.
    """
    assert os.path.exists(path), "File path should exist"
    assert os.stat(path).st_size > 0, "File must contain at least one record"

    with open(path, "r") as file_stream:
        last_line = file_stream.readlines()[-1]
        return ScanRecord.from_str(last_line)


def overwrite_scan_record(path, scan_record):
    """
    Overwrites the file denoted by path and records the the new scan in the
    file. The scan time period (start & stop), input directory, and last searched
    path are written as a single line to the file.
    """
    assert os.path.exists(path), "File path should exist"
    
    with open(path, "w") as file:
        file.write(str(scan_record)+"\n")


def append_scan_record(path, scan_record):
    """
    Writes successful scan information to the scan history file denoted by
    the path. The scan time period (start & stop), input directory, and last searched
    path are written as a single line to the file.
    """
    assert os.path.exists(path), "File path should exist"

    with open(path, "a") as file:
        file.write(str(scan_record)+"\n")

