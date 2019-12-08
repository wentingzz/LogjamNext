"""
@author Nathaniel Brooks

Tests the functionality from the incremental.py file.
"""


import unittest
import os
import time
import stat
import shutil

import paths
import incremental



CODE_SRC_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(CODE_SRC_DIR, "test-data", "Incremental")


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
        
        with self.assertRaises(NotImplementedError, msg=""):
            ans = (record == 23)
            self.fail("Should not have allowed comparison")
    
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
        self.tmp_dir = os.path.join(CODE_SRC_DIR, tmp_name)
        os.mkdir(self.tmp_dir)
        self.history_dir = os.path.join(self.tmp_dir, "history")
        os.mkdir(self.history_dir)
        self.scratch_dir = os.path.join(self.tmp_dir, "scratch")
        os.mkdir(self.scratch_dir)
        self.input_dir = os.path.join(self.tmp_dir, "input")
        os.mkdir(self.input_dir)
    
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
        
        shutil.rmtree(self.history_dir)                 # clean up last Scan construction
        os.makedirs(self.history_dir, exist_ok=True)
        
        history_active_file = os.path.join(self.history_dir, "scan-history-active.txt")
        open(history_active_file, "x").close()
        record = incremental.ScanRecord.from_str('0 1000 "." "./log.txt"')
        incremental.overwrite_scan_record(history_active_file, record)
        
        scan = incremental.Scan(".", self.history_dir, self.scratch_dir)
        
        self.assertEqual(".", scan.input_dir)
        self.assertEqual(self.history_dir, scan.history_dir)
        self.assertEqual(0, scan.time_period.start)
        self.assertEqual(1000, scan.time_period.stop)
        self.assertEqual("./log.txt", scan.last_path)
    
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
    
    def test_should_consider_entry(self):
        history_active_file = os.path.join(self.history_dir, "scan-history-active.txt")
        open(history_active_file, "x").close()
        record = incremental.ScanRecord(1000, 2000, self.input_dir, os.path.join(self.input_dir,"AAA.txt"))
        incremental.overwrite_scan_record(history_active_file, record)
        
        scan = incremental.Scan(self.input_dir, self.history_dir, self.scratch_dir)
        self.assertEqual(self.input_dir, scan.input_dir)
        self.assertEqual(1000, scan.time_period.start)
        self.assertEqual(2000, scan.time_period.stop)
        
        old_file = paths.QuantumEntry(self.input_dir, "OOO.txt")
        open(old_file.abspath, "a").close()
        os.utime(old_file.abspath, (time.time(), 0))
        self.assertFalse(scan.should_consider_entry(old_file))
        
        cur_file = paths.QuantumEntry(self.input_dir, "CCC.txt")
        open(cur_file.abspath, "a").close()
        os.utime(cur_file.abspath, (time.time(), 1500))
        self.assertTrue(scan.should_consider_entry(cur_file))
        
        new_file = paths.QuantumEntry(self.input_dir, "NNN.txt")
        open(new_file.abspath, "a").close()
        os.utime(new_file.abspath, (time.time(), 3000))
        self.assertFalse(scan.should_consider_entry(new_file))
        
        dir = paths.QuantumEntry(self.input_dir, "DDD")
        os.makedirs(dir.abspath, exist_ok=True)
        self.assertTrue(scan.should_consider_entry(dir))

    def test_complete_scan(self):
        history_active_file = os.path.join(self.history_dir, "scan-history-active.txt")
        open(history_active_file, "x").close()
        record = incremental.ScanRecord(3000, 5000, self.input_dir, os.path.join(self.input_dir,"log.txt"))
        incremental.overwrite_scan_record(history_active_file, record)
        
        scan = incremental.Scan(self.input_dir, self.history_dir, self.scratch_dir)
        self.assertEqual(self.input_dir, scan.input_dir)
        self.assertEqual(3000, scan.time_period.start)
        self.assertEqual(5000, scan.time_period.stop)
        
        scan.complete_scan()
        
        last_record = incremental.extract_last_scan_record(history_active_file)
        self.assertEqual(3000, last_record.time_period.start)
        self.assertEqual(5000, last_record.time_period.stop)
        self.assertEqual(self.input_dir, last_record.input_dir)
        self.assertEqual("", last_record.last_path)
        self.assertTrue(last_record.is_complete())
    
    def test_premature_exit(self):
        pass
        
    

class ScanHelperFuncTestCase(unittest.TestCase):
    """ Tests the basic helper functions used by the Scan class """
    
    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(CODE_SRC_DIR, tmp_name)
        os.makedirs(self.tmp_dir)
        self.assertTrue(os.path.isdir(self.tmp_dir))
    
    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        self.assertTrue(not os.path.exists(self.tmp_dir))
    
    def test_sorted_recursive_order(self):
        walk_pattern = [
            "/zaa",
            "/z/f",
            "/z/b",
            "/z/a",
            "/z",
            "/z-a",
            "/x.txt",
            "/dir3/X.txt",
            "/dir3",
            "/dir2/nested/b.txt",
            "/dir2/nested/a.txt",
            "/dir2",
            "/dir1/z.txt",
            "/dir1/y.txt",
            "/dir1/x.txt",
            "/dir1",
            "/dir/x.txt",
            "/dir/dir/c.txt",
            "/dir/dir/b.txt",
            "/dir/dir/a.txt",
            "/dir/dir-a.txt",
            "/dir/b.txt",
            "/dir",
            "/dir-dir-a.txt",
            "/a.txt",
        ]
        
        self.assertEqual(walk_pattern, incremental.sorted_recursive_order(walk_pattern))
    
    def test_list_unscanned_entries(self):
        file_structure = {
            "fileX.txt" : {},
            
            "dir3":
            {
                "nested":
                {
                    "fileC.txt" : {},
                    "fileB.txt" : {},
                    "fileA.txt" : {},
                },
            },
            
            "dir2":
            {
                "fileC.txt" : {},
                "fileB.txt" : {},
                "fileA.txt" : {},
            },
            
            "dir":
            {
                "nested":
                {
                    "fileC.txt" : {},
                    "fileB.txt" : {},
                    "fileA.txt" : {},
                },
            },
        }
        
        def recursive_build(dir, dic):
            for key in dic:
                if len(dic[key]) == 0:
                    new_file_path = os.path.join(dir, key)
                    open(new_file_path, "a").close()
                else:
                    new_dir_path = os.path.join(dir, key)
                    os.makedirs(new_dir_path)
                    recursive_build(new_dir_path, dic[key])
        
        recursive_build(self.tmp_dir, file_structure)
        
        if True:
            # Search the base directory
            search_dir = paths.QuantumEntry(self.tmp_dir, "")
            
            # Nothing scanned so far, return all
            i = incremental.list_unscanned_entries(search_dir, "")
            for relpath in ["fileX.txt", "dir3", "dir2", "dir"]:
                self.assertEqual(relpath, next(i).relpath.replace("\\","/"))
            
            # fileX.txt scanned, return all except that
            i = incremental.list_unscanned_entries(search_dir, "fileX.txt")
            for relpath in ["dir3", "dir2", "dir"]:
                self.assertEqual(relpath, next(i).relpath.replace("\\","/"))
            
            # Child of dir3 scanned, return all (dir3 needs to continue scanning)
            i = incremental.list_unscanned_entries(search_dir, "dir3/nested")
            for relpath in ["dir3", "dir2", "dir"]:
                self.assertEqual(relpath, next(i).relpath.replace("\\","/"))
            
            # Child of dir3 scanned, return all (dir3 needs to continue scanning)
            i = incremental.list_unscanned_entries(search_dir, "dir3/nested/fileA.txt")
            for relpath in ["dir3", "dir2", "dir"]:
                self.assertEqual(relpath, next(i).relpath.replace("\\","/"))
            
            # dir3 fully scanned, return all except dir3
            i = incremental.list_unscanned_entries(search_dir, "dir3")
            for relpath in ["dir2", "dir"]:
                self.assertEqual(relpath, next(i).relpath.replace("\\","/"))
            
            # Child of dir2 scanned, return all except dir3 (dir2 needs to continue)
            i = incremental.list_unscanned_entries(search_dir, "dir2/fileA.txt")
            for relpath in ["dir2", "dir"]:
                self.assertEqual(relpath, next(i).relpath.replace("\\","/"))
            
            # dir2 fully scanned, return all except dir3, dir2
            i = incremental.list_unscanned_entries(search_dir, "dir2")
            for relpath in ["dir"]:
                self.assertEqual(relpath, next(i).relpath.replace("\\","/"))

            # Child of dir scanned, return dir b/c it needs to continue scanning
            i = incremental.list_unscanned_entries(search_dir, "dir/nested")
            for relpath in ["dir"]:
                self.assertEqual(relpath, next(i).relpath.replace("\\","/"))
            
            # dir fully scanned, return nothing, nothing left to scan
            i = incremental.list_unscanned_entries(search_dir, "dir")
            try:
                next(i)
                self.fail("Should have raised an exception")
            except StopIteration:
                pass
        
        if True:
            # Search the nested directory
            search_dir.relpath = "dir3/nested"
            
            # Nothing scanned so far, return all
            i = incremental.list_unscanned_entries(search_dir, "")
            for relpath in ["dir3/nested/fileC.txt", "dir3/nested/fileB.txt", "dir3/nested/fileA.txt"]:
                self.assertEqual(relpath, next(i).relpath.replace("\\","/"))
            
            # fileC.txt scanned, return all except fileC.txt
            i = incremental.list_unscanned_entries(search_dir, "dir3/nested/fileC.txt")
            for relpath in ["dir3/nested/fileB.txt", "dir3/nested/fileA.txt"]:
                self.assertEqual(relpath, next(i).relpath.replace("\\","/"))
            
            # fileA.txt scanned, return nothing (all done)
            i = incremental.list_unscanned_entries(search_dir, "dir3/nested/fileA.txt")
            try:
                next(i)
                self.fail("Should have raised an exception")
            except StopIteration:
                pass
            
            # nested scanned, return nothing (all done)
            i = incremental.list_unscanned_entries(search_dir, "dir3/nested")
            try:
                next(i)
                self.fail("Should have raised an exception")
            except StopIteration:
                pass
            
            # sibling directory scanned, return nothing (all done)
            i = incremental.list_unscanned_entries(search_dir, "dir2")
            try:
                next(i)
                self.fail("Should have raised an exception")
            except StopIteration:
                pass
            
            # sibling directory scanned, return nothing (all done)
            i = incremental.list_unscanned_entries(search_dir, "dir")
            try:
                next(i)
                self.fail("Should have raised an exception")
            except StopIteration:
                pass
        
        return
    
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

