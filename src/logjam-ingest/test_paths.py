"""
@author Nathaniel Brooks

Tests the features found in the paths.py file.
"""


import unittest
import os
import time
import shutil
import tarfile
import stat
import gzip
import subprocess

import paths


CODE_SRC_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(CODE_SRC_DIR, "test-data", "Paths")


class QuantumEntryTestCase(unittest.TestCase):
    """ Test case for the QuantumEntry object """
    
    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(CODE_SRC_DIR, tmp_name)
        os.makedirs(self.tmp_dir)
        self.assertTrue(os.path.isdir(self.tmp_dir))
    
    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        self.assertTrue(not os.path.exists(self.tmp_dir))
    
    def test_init(self):
        entry = paths.QuantumEntry("/mnt/nfs", "2001387465/dir/dir/log.txt")
        self.assertEqual("/mnt/nfs", entry.srcpath)
        self.assertEqual("2001387465/dir/dir/log.txt", entry.relpath)
        self.assertEqual("/mnt/nfs/2001387465/dir/dir/log.txt", entry.abspath)
        self.assertEqual("/mnt/nfs/2001387465/dir/dir", entry.absdirpath)
        self.assertEqual("2001387465/dir/dir", entry.reldirpath)
        self.assertEqual("log.txt", entry.basename)
        self.assertEqual("log", entry.filename)
        self.assertEqual(".txt", entry.extension)
        
        entry = paths.QuantumEntry(".", "dir/dir/dir/")
        self.assertEqual(".", entry.srcpath)
        self.assertEqual("dir/dir/dir", entry.relpath)
        self.assertNotEqual("./dir/dir/dir", entry.abspath)     # can't know absolute path
        self.assertNotEqual("./dir/dir", entry.absdirpath)      # can't know absolute path
        self.assertEqual("dir/dir", entry.reldirpath)
        self.assertEqual("dir", entry.basename)
        self.assertEqual("dir", entry.filename)
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("./", "dir/dir/dir/")
        self.assertEqual(".", entry.srcpath)
        self.assertEqual("dir/dir/dir", entry.relpath)
        self.assertNotEqual("./dir/dir/dir", entry.abspath)     # can't know absolute path
        self.assertNotEqual("./dir/dir", entry.absdirpath)      # can't know absolute path
        self.assertEqual("dir/dir", entry.reldirpath)
        self.assertEqual("dir", entry.basename)
        self.assertEqual("dir", entry.filename)
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/", "dir/dir")
        self.assertEqual("/", entry.srcpath)
        self.assertEqual("dir/dir", entry.relpath)
        self.assertEqual("/dir/dir", entry.abspath)
        self.assertEqual("/dir", entry.absdirpath)
        self.assertEqual("dir", entry.reldirpath)
        self.assertEqual("dir", entry.basename)
        self.assertEqual("dir", entry.filename)
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/", "./dir/dir")
        self.assertEqual("/", entry.srcpath)
        self.assertEqual("./dir/dir", entry.relpath)
        self.assertEqual("/dir/dir", entry.abspath)
        self.assertEqual("/dir", entry.absdirpath)
        self.assertEqual("./dir", entry.reldirpath)
        self.assertEqual("dir", entry.basename)
        self.assertEqual("dir", entry.filename)
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/mnt/", "../dir/dir/")
        self.assertEqual("/mnt", entry.srcpath)
        self.assertEqual("../dir/dir", entry.relpath)
        self.assertEqual("/dir/dir", entry.abspath)
        self.assertEqual("/dir", entry.absdirpath)
        self.assertEqual("../dir", entry.reldirpath)
        self.assertEqual("dir", entry.basename)
        self.assertEqual("dir", entry.filename)
        self.assertEqual("", entry.extension)
        
        try:
            entry = paths.QuantumEntry("/", "/dir/dir")
            self.fail()
        except AssertionError:
            pass                                                # no leading / on rel
    
    def test_eq(self):
        entry = paths.QuantumEntry("/mnt/nfs/", "2008938201/log.txt")
        self.assertEqual(paths.QuantumEntry("/mnt/nfs/", "2008938201/log.txt"), entry)
        self.assertEqual(paths.QuantumEntry("/mnt/nfs", "2008938201/log.txt"), entry)
        self.assertTrue(paths.QuantumEntry("/mnt/nfs/", "2008938201/log.txt") == entry)
        self.assertTrue(paths.QuantumEntry("/mnt/nfs", "2008938201/log.txt") == entry)
        
        self.assertNotEqual(paths.QuantumEntry("/mnt/nfs/", "2008938201/"), entry)
        self.assertNotEqual(paths.QuantumEntry("/mnt/nfs", "2008938201"), entry)
        self.assertFalse(paths.QuantumEntry("/mnt/nfs/", "2008938201/") == entry)
        self.assertFalse(paths.QuantumEntry("/mnt/nfs", "2008938201") == entry)
    
    def test_truediv(self):
        entry = paths.QuantumEntry("/", "dir/dir")
        self.assertEqual(paths.QuantumEntry("/", "dir/dir/dir"), entry / "dir")
        self.assertEqual(paths.QuantumEntry("/", "dir/dir/a/b/c"), entry / "a/b/c/")
        self.assertEqual(paths.QuantumEntry("/", "dir/dir/a/b/c/"), entry / "a/b/c")
        self.assertEqual(paths.QuantumEntry("/", "dir/dir/a/b/c"), entry / "a/b/c")
        self.assertEqual(paths.QuantumEntry("/", "dir/dir"), entry)
    
    def test_itruediv(self):
        entry = paths.QuantumEntry("/", "./")
        self.assertEqual(".", entry.relpath)
        
        entry /= "dir/dir/"
        self.assertEqual("./dir/dir", entry.relpath)
        
        entry /= "../tmp"
        self.assertEqual("./dir/dir/../tmp", entry.relpath)
        self.assertEqual("/dir/tmp", entry.abspath)
    
    def test_extension(self):
        entry = paths.QuantumEntry("/", "dir/dir")
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/", "dir/dir/")
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/", ".git")
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/", ".git/")
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/", ".git/dir")
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/", ".git/dir/")
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/", "john.smith/dir")
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/", "john.smith/dir/")
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/", "log.txt")
        self.assertEqual(".txt", entry.extension)
        
        entry = paths.QuantumEntry("/", "log.txt/")
        self.assertEqual(".txt", entry.extension)
        
        entry = paths.QuantumEntry("/", ".dir/dir.dir.dir/")
        self.assertEqual(".dir", entry.extension)
        
        entry = paths.QuantumEntry("/", ".dir/dir.dir.dir/.dir")
        self.assertEqual("", entry.extension)
        
        entry = paths.QuantumEntry("/", ".dir/dir.tar.gz")
        self.assertEqual(".gz", entry.extension)
    
    def test_exists(self):
        entry = paths.QuantumEntry(self.tmp_dir, str(int(time.time())))
        self.assertFalse(entry.exists())
        self.assertFalse(entry.is_dir())
        self.assertFalse(entry.is_file())
    
    def test_is_dir(self):
        folder_path = os.path.join(self.tmp_dir, "folder")
        os.makedirs(folder_path)
        self.assertTrue(os.path.exists(folder_path))
        self.assertTrue(os.path.isdir(folder_path))
        
        entry = paths.QuantumEntry(self.tmp_dir, "folder")
        self.assertTrue(entry.exists())
        self.assertTrue(entry.is_dir())
        self.assertFalse(entry.is_file())
    
    def test_is_file(self):
        file_path = os.path.join(self.tmp_dir, "log.txt")
        open(file_path, "a").close()
        self.assertTrue(os.path.exists(file_path))
        self.assertTrue(os.path.isfile(file_path))
        
        entry = paths.QuantumEntry(self.tmp_dir, "log.txt")
        self.assertTrue(entry.exists())
        self.assertFalse(entry.is_dir())
        self.assertTrue(entry.is_file())
    


