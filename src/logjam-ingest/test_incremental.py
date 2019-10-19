"""
@author Nathaniel Brooks
Tests the functionality from the incremental.py file.
"""


import unittest
import os
import time
import stat


import incremental


class TimePeriodTestCase(unittest.TestCase):
    """ Tests the TimePeriod class for its basic functionality.
    """
    
    def test_init(self):
        tmp = incremental.TimePeriod(0,1)
        self.assertEqual(0, tmp.start)
        self.assertEqual(1, tmp.stop)
        
        self.assertRaises(AssertionError, incremental.TimePeriod, 1, 0)


class ScanRecordTestCase(unittest.TestCase):
    """ Tests the ScanRecord class for its basic functionality.
    """
    
    def test_init(self):
        tmp = incremental.ScanRecord('100200 200300 "/path/to/input" "./relative/path"')
        self.assertEqual(incremental.TimePeriod(100200, 200300), tmp.time_period)
        self.assertEqual(100200, tmp.time_period.start)
        self.assertEqual(200300, tmp.time_period.stop)
        self.assertEqual("/path/to/input", tmp.input_dir)
        self.assertEqual("./relative/path", tmp.last_path)
        
        tmp = incremental.ScanRecord('1 2 "/path w/ spaces" "log.txt"')
        self.assertEqual(incremental.TimePeriod(1, 2), tmp.time_period)
        self.assertEqual(1, tmp.time_period.start)
        self.assertEqual(2, tmp.time_period.stop)
        self.assertEqual("/path w/ spaces", tmp.input_dir)
        self.assertEqual("log.txt", tmp.last_path)
        
        tmp = incremental.ScanRecord('0 1 "/nfs" ""')
        self.assertEqual(incremental.TimePeriod(0, 1), tmp.time_period)
        self.assertEqual(0, tmp.time_period.start)
        self.assertEqual(1, tmp.time_period.stop)
        self.assertEqual("/nfs", tmp.input_dir)
        self.assertEqual("", tmp.last_path)
        
        self.assertRaises(AssertionError, incremental.ScanRecord, '1 0 "" ""')
        self.assertRaises(AssertionError, incremental.ScanRecord, '01""""')
        self.assertRaises(AssertionError, incremental.ScanRecord, '0 1""""')
        self.assertRaises(AssertionError, incremental.ScanRecord, '0 1 """"')
        self.assertRaises(AssertionError, incremental.ScanRecord, '0 1 /path file.txt')
    
    def test_str(self):
        tmp = incremental.ScanRecord('5 7 "/nfs" "./2001938907/log.txt"')
        self.assertEqual('5 7 "/nfs" "./2001938907/log.txt"', str(tmp))
        
        tmp = incremental.ScanRecord('5 7 "/path w/ spaces" "log.txt"')
        self.assertEqual('5 7 "/path w/ spaces" "log.txt"', str(tmp))
    
    def test_is_complete(self):
        tmp = incremental.ScanRecord('0 1 "/nfs" "./folder/log.txt"')
        self.assertFalse(tmp.is_complete())
        
        tmp = incremental.ScanRecord('0 1 "/nfs" ""')
        self.assertTrue(tmp.is_complete())


class ScanTestCase(unittest.TestCase):
    """ Tests the business logic of the Scan class.
    """
    
    def test_init(self):
        return

