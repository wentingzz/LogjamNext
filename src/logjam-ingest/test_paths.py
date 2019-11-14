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


class EntryTestCase(unittest.TestCase):
    """ Test case for the Entry object """
    
    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(CODE_SRC_DIR, tmp_name)
        os.makedirs(self.tmp_dir)
        self.assertTrue(os.path.isdir(self.tmp_dir))
    
    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        self.assertTrue(not os.path.exists(self.tmp_dir))
    
    def test_init(self):
        entry = paths.Entry("/mnt/nfs", "2001387465/dir/dir/log.txt")
        self.assertEqual("/mnt/nfs/", entry.srcpath)
        self.assertEqual("2001387465/dir/dir/log.txt", entry.relpath)
        self.assertEqual("/mnt/nfs/2001387465/dir/dir/log.txt", entry.fullpath)
        self.assertEqual("/mnt/nfs/2001387465/dir/dir/log.txt", entry.abspath)
        
        entry = paths.Entry(".", "dir/dir/dir/")
        self.assertEqual("./", entry.srcpath)
        self.assertEqual("dir/dir/dir/", entry.relpath)
        self.assertEqual("./dir/dir/dir/", entry.fullpath)
        self.assertNotEqual("./dir/dir/dir/", entry.abspath)    # can't know absolute path
        
        entry = paths.Entry("./", "dir/dir/dir/")
        self.assertEqual("./", entry.srcpath)
        self.assertEqual("dir/dir/dir/", entry.relpath)
        self.assertEqual("./dir/dir/dir/", entry.fullpath)
        self.assertNotEqual("./dir/dir/dir/", entry.abspath)    # can't know absolute path
        
        entry = paths.Entry("/", "dir/dir")
        self.assertEqual("/", entry.srcpath)
        self.assertEqual("dir/dir", entry.relpath)
        self.assertEqual("/dir/dir", entry.fullpath)
        self.assertNotEqual("/dir/dir", entry.abspath)
        
        try:
            entry = paths.Entry("/", "/dir/dir")
            self.fail()
        except AssertionError:
            pass                                                # no leading / on rel
        
        try:
            entry = paths.Entry("/", "./dir/dir")
            self.fail()
        except AssertionError:
            pass                                                # no leading ./ on rel
        
        try:
            entry = paths.Entry("/", "../dir/dir")
            self.fail()
        except AssertionError:
            pass                                                # no leading ../ on rel
    
    def test_exists(self):
        entry = paths.Entry(self.tmp_dir, str(int(time.time())))
        self.assertFalse(entry.exists())
        self.assertFalse(entry.isdir())
        self.assertFalse(entry.isfile())
    
    def test_isdir(self):
        folder_path = os.path.join(self.tmp_dir, "folder/")
        os.makedirs(folder_path)
        self.assertTrue(os.path.exists(folder_path))
        self.assertTrue(os.path.isdir(folder_path))
        
        entry = paths.Entry(self.tmp_dir, "folder/")
        self.assertTrue(entry.exists())
        self.assertTrue(entry.isdir())
        self.assertFalse(entry.isfile())
    
    def test_isfile(self):
        file_path = os.path.join(self.tmp_dir, "log.txt")
        open(file_path, "a").close()
        self.assertTrue(os.path.exists(file_path))
        self.assertTrue(os.path.isfile(file_path))
        
        entry = paths.Entry(self.tmp_dir, "log.txt")
        self.assertTrue(entry.exists())
        self.assertFalse(entry.isdir())
        self.assertTrue(entry.isfile())
    
    def test_extension(self):
        entry = paths.Entry("/", "dir/dir")
        self.assertEqual("", entry.extension)
        
        entry = paths.Entry("/", "dir/dir/")
        self.assertEqual("", entry.extension)
        
        entry = paths.Entry("/", ".git")
        self.assertEqual("", entry.extension)
        
        entry = paths.Entry("/", ".git/")
        self.assertEqual("", entry.extension)
        
        entry = paths.Entry("/", ".git/dir")
        self.assertEqual("", entry.extension)
        
        entry = paths.Entry("/", ".git/dir/")
        self.assertEqual("", entry.extension)
        
        entry = paths.Entry("/", "john.smith/dir")
        self.assertEqual("", entry.extension)
        
        entry = paths.Entry("/", "john.smith/dir/")
        self.assertEqual("", entry.extension)
        
        entry = paths.Entry("/", "log.txt")
        self.assertEqual(".txt", entry.extension)
        
        entry = paths.Entry("/", "log.txt/")
        self.assertEqual(".txt", entry.extension)

