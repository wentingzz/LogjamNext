"""
@author Nathaniel Brooks

Tests the functionality from the incremental.py file.
"""


import unittest
import os
import time
import stat
import shutil

import incremental


code_src_dir = os.path.dirname(os.path.realpath(__file__))


class TimePeriodTestCase(unittest.TestCase):
    """ Tests the TimePeriod class for its basic functionality. """
    
    def test_init(self):
        tmp = incremental.TimePeriod(0,1)
        self.assertEqual(0, tmp.start)
        self.assertEqual(1, tmp.stop)
        
        self.assertRaises(AssertionError, incremental.TimePeriod, 1, 0)
    
    def test_eq(self):
        tmp = incremental.TimePeriod(0,1)
        self.assertEqual(incremental.TimePeriod(0,1), tmp)
        self.assertNotEqual(incremental.TimePeriod(0,2), tmp)
        self.assertEqual(incremental.TimePeriod(123,200),incremental.TimePeriod(123,200))
        self.assertEqual(tmp,tmp)
        
        self.assertRaises(NotImplementedError, incremental.TimePeriod.__eq__, tmp, "X")
    
    def test_str(self):
        self.assertEqual("-50 100", str(incremental.TimePeriod(-50,100)))
        self.assertEqual("0 200", str(incremental.TimePeriod(0,200)))
    
    def test_contains(self):
        tmp = incremental.TimePeriod(10,20)
        
        for x in range(0,10):
            self.assertFalse(x in tmp)
        for x in range(10,20):
            self.assertTrue(x in tmp)
        for x in range(20,30):
            self.assertFalse(x in tmp)
    
    def test_ancient_history(self):
        self.assertEqual(-60*60*24*365*70, incremental.TimePeriod.ancient_history())


class ScanRecordTestCase(unittest.TestCase):
    """ Tests the ScanRecord class for its basic functionality. """
    
    def test_from_str(self):
        record = incremental.ScanRecord.from_str('100200 200300 "/path/to/input" "./relative/path"')
        self.assertEqual(incremental.TimePeriod(100200, 200300), record.time_period)
        self.assertEqual(100200, record.time_period.start)
        self.assertEqual(200300, record.time_period.stop)
        self.assertEqual("/path/to/input", record.input_dir)
        self.assertEqual("./relative/path", record.last_path)
        
        record = incremental.ScanRecord.from_str('1 2 "/path w/ spaces" "log.txt"')
        self.assertEqual(incremental.TimePeriod(1, 2), record.time_period)
        self.assertEqual(1, record.time_period.start)
        self.assertEqual(2, record.time_period.stop)
        self.assertEqual("/path w/ spaces", record.input_dir)
        self.assertEqual("log.txt", record.last_path)
        
        record = incremental.ScanRecord.from_str('0 1 "/nfs" ""')
        self.assertEqual(incremental.TimePeriod(0, 1), record.time_period)
        self.assertEqual(0, record.time_period.start)
        self.assertEqual(1, record.time_period.stop)
        self.assertEqual("/nfs", record.input_dir)
        self.assertEqual("", record.last_path)
        
        self.assertRaises(AssertionError, incremental.ScanRecord.from_str, '1 0 "" ""')
        self.assertRaises(AssertionError, incremental.ScanRecord.from_str, '01""""')
        self.assertRaises(AssertionError, incremental.ScanRecord.from_str, '0 1""""')
        self.assertRaises(AssertionError, incremental.ScanRecord.from_str, '0 1 """"')
        self.assertRaises(AssertionError, incremental.ScanRecord.from_str, '0 1 /path file.txt')
    
    def test_init(self):
        record = incremental.ScanRecord(100200, 200300, "/path/to/input", "./relative/path")
        self.assertEqual(incremental.TimePeriod(100200, 200300), record.time_period)
        self.assertEqual(100200, record.time_period.start)
        self.assertEqual(200300, record.time_period.stop)
        self.assertEqual("/path/to/input", record.input_dir)
        self.assertEqual("./relative/path", record.last_path)
        
        record = incremental.ScanRecord(1, 2, "/path w/ spaces", "log.txt")
        self.assertEqual(incremental.TimePeriod(1, 2), record.time_period)
        self.assertEqual(1, record.time_period.start)
        self.assertEqual(2, record.time_period.stop)
        self.assertEqual("/path w/ spaces", record.input_dir)
        self.assertEqual("log.txt", record.last_path)
        
        record = incremental.ScanRecord(0, 1, "/nfs", "")
        self.assertEqual(incremental.TimePeriod(0, 1), record.time_period)
        self.assertEqual(0, record.time_period.start)
        self.assertEqual(1, record.time_period.stop)
        self.assertEqual("/nfs", record.input_dir)
        self.assertEqual("", record.last_path)
    
    def test_eq(self):
        record = incremental.ScanRecord.from_str('3 4 "." "X"')
        
        self.assertEqual(record, record)
        self.assertEqual(record, incremental.ScanRecord.from_str('3 4 "." "X"'))
        self.assertEqual(record, incremental.ScanRecord(3, 4, ".", "X"))
    
    def test_str(self):
        record = incremental.ScanRecord.from_str('5 7 "/nfs" "./2001938907/log.txt"')
        self.assertEqual('5 7 "/nfs" "./2001938907/log.txt"', str(record))
        
        record = incremental.ScanRecord.from_str('5 7 "/path w/ spaces" "log.txt"')
        self.assertEqual('5 7 "/path w/ spaces" "log.txt"', str(record))
        
        record = incremental.ScanRecord.from_str('0 1 "." "x"')
        self.assertEqual(record, incremental.ScanRecord.from_str(str(record)))
    
    def test_is_complete(self):
        record = incremental.ScanRecord.from_str('0 1 "/nfs" "./folder/log.txt"')
        self.assertFalse(record.is_complete())
        
        record = incremental.ScanRecord.from_str('0 1 "/nfs" ""')
        self.assertTrue(record.is_complete())


class ScanTestCase(unittest.TestCase):
    """ Tests the business logic of the Scan class. """
    
    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(code_src_dir, tmp_name)
        os.mkdir(self.tmp_dir)
        self.history_dir = os.path.join(self.tmp_dir, "history")
        os.mkdir(self.history_dir)
        self.scratch_dir = os.path.join(self.tmp_dir, "scratch")
        os.mkdir(self.scratch_dir)
    
    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        self.assertTrue(not os.path.exists(self.tmp_dir))
        self.assertTrue(not os.path.exists(self.history_dir))
        self.assertTrue(not os.path.exists(self.scratch_dir))
    
    def test_init(self):
        scan = incremental.Scan(".",self.history_dir,self.scratch_dir)# assumes "." is a valid path
        cur_time = time.time()                          # assumes both same time source
        
        self.assertGreater(cur_time, scan.safe_time)
        self.assertEqual(".", scan.input_dir)
        self.assertEqual(self.history_dir, scan.history_dir)
        
        self.assertEqual(incremental.TimePeriod.ancient_history(), scan.time_period.start)
        self.assertEqual(scan.safe_time, scan.time_period.stop)
        self.assertEqual("", scan.last_path)
    
    def test_update_from_scan_record(self):
        scan = incremental.Scan(".",self.history_dir,self.scratch_dir)
        
        self.assertEqual(incremental.TimePeriod.ancient_history(), scan.time_period.start)
        self.assertEqual(scan.safe_time, scan.time_period.stop)
        self.assertEqual("", scan.last_path)
        
        scan._update_from_scan_record(incremental.ScanRecord.from_str('0 1000 "." ""'))
        
        self.assertEqual(1000, scan.time_period.start)
        self.assertEqual(scan.safe_time, scan.time_period.stop)
        self.assertEqual("", scan.last_path)
        
        scan._update_from_scan_record(incremental.ScanRecord.from_str('-50 5000 "." "./log.txt"'))
        
        self.assertEqual(-50, scan.time_period.start)
        self.assertEqual(5000, scan.time_period.stop)
        self.assertEqual("./log.txt", scan.last_path)
    
    def test_to_scan_record(self):
        scan = incremental.Scan(".",self.history_dir,self.scratch_dir)
        
        scan.last_path = ""
        
        record = scan._to_scan_record()
        self.assertEqual(incremental.TimePeriod.ancient_history(), record.time_period.start)
        self.assertEqual(scan.safe_time, record.time_period.stop)
        self.assertEqual(".", record.input_dir)
        self.assertEqual("", record.last_path)
        self.assertTrue(record.is_complete())
        
        scan.last_path = "./log.txt"
        
        record = scan._to_scan_record()
        self.assertEqual(incremental.TimePeriod.ancient_history(), record.time_period.start)
        self.assertEqual(scan.safe_time, record.time_period.stop)
        self.assertEqual(".", record.input_dir)
        self.assertEqual("./log.txt", record.last_path)
        self.assertFalse(record.is_complete())


class ScanHelperFuncTestCase(unittest.TestCase):
    """ Tests the basic helper functions used by the Scan class """
    
    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(code_src_dir, tmp_name)
        os.mkdir(self.tmp_dir)
    
    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
    
    def test_overwrite_scan_record(self):
        record = incremental.ScanRecord.from_str('0 10 "/nfs" ""')
        active_file = os.path.join(self.tmp_dir, "active.txt")
        
        open(active_file, "a").close()
        self.assertTrue(os.path.isfile(active_file))
        
        incremental.overwrite_scan_record(active_file, record)
        with open(active_file, "r") as f:
            self.assertEqual(str(record)+"\n", f.read())
    
    def test_append_scan_record(self):
        record = incremental.ScanRecord.from_str('0 10 "/nfs" ""')
        history_file = os.path.join(self.tmp_dir, "history.txt")
        
        open(history_file, "a").close()
        self.assertTrue(os.path.isfile(history_file))
        
        incremental.append_scan_record(history_file, record)
        incremental.append_scan_record(history_file, record)
        incremental.append_scan_record(history_file, record)
        with open(history_file, "r") as f:
            self.assertEqual((str(record)+"\n")*3, f.read())
    
    def test_extract_last_scan_record(self):
        record = incremental.ScanRecord.from_str('0 10 "/nfs" ""')
        active_file = os.path.join(self.tmp_dir, "active.txt")
        
        with open(active_file, "a") as f:
            f.write(str(record) + "\n")
        
        self.assertEqual(record, incremental.extract_last_scan_record(active_file))

