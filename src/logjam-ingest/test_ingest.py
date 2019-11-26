"""
@author Jeremy Schmidt
@author Nathaniel Brooks

Unit tests for top-level ingestion script
"""


import os
import shutil
import time
import unittest
import sqlite3
import gzip

import ingest
import paths


CODE_SRC_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(CODE_SRC_DIR, "test-data", "Scan")


class FullIngestTestCase(unittest.TestCase):
    """ Test case class for ingest unit tests """

    data_dir = os.path.join(CODE_SRC_DIR, "test-data")

    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(CODE_SRC_DIR, tmp_name)
        os.mkdir(self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)


    def test_basic_ingest(self):
        """ Run the full ingest process on a simple set of inputs """
        # Establish paths under the test's temp directory
        input_dir = os.path.join(self.data_dir, "Scan")
        categ_dir = os.path.join(self.tmp_dir, "categories")
        scratch_dir = os.path.join(self.tmp_dir, "scratch")
        history_dir = os.path.join(self.tmp_dir, "history")

        for (basepath, dirs, files) in os.walk(input_dir):          # make files old
            for file in files:
                os.utime(os.path.join(basepath,file), times=(time.time(),0))

        # Run ingest on sample data
        ingest.ingest_log_files(input_dir, scratch_dir, history_dir)

        # TODO: Verify ingest worked now that category folders are gone
        
        return


class RecursiveHelperFuncTestCase(unittest.TestCase):
    """ Test case class for recursive helper functions """
    
    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(CODE_SRC_DIR, tmp_name)
        os.makedirs(self.tmp_dir)
        self.assertTrue(os.path.isdir(self.tmp_dir))
    
    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        self.assertTrue(not os.path.exists(self.tmp_dir))

    def test_unzip_into_scratch_dir(self):
        
        input_dir = os.path.join(self.tmp_dir, "mnt/nfs")
        os.makedirs(input_dir, exist_ok=True)
        
        scratch_dir = os.path.join(self.tmp_dir, "tmp/scratch_space1777")
        os.makedirs(scratch_dir, exist_ok=True)
        
        
        # shutil.make_archive(
        #    base_name="archiveA",
        #    format="zip",
        #    root_dir=input_dir
        
        archiveB_gz = paths.QuantumEntry(input_dir, "archiveB.txt.gz")
        with gzip.open(archiveB_gz.abspath, "wb") as fd:
            fd.write("This is a GZIP file\n".encode())
            
        self.assertTrue(os.path.exists(os.path.join(input_dir, "archiveB.txt.gz")))
        self.assertFalse(os.path.exists(os.path.join(scratch_dir, "archiveB.txt.gz")))
        self.assertFalse(os.path.exists(os.path.join(input_dir, "archiveB.txt")))
        self.assertFalse(os.path.exists(os.path.join(scratch_dir, "archiveB.txt")))
        
        archiveB_txt = ingest.unzip_into_scratch_dir(input_dir, scratch_dir, archiveB_gz)
        
        self.assertTrue(os.path.exists(os.path.join(input_dir, "archiveB.txt.gz")))
        self.assertFalse(os.path.exists(os.path.join(scratch_dir, "archiveB.txt.gz")))
        self.assertFalse(os.path.exists(os.path.join(input_dir, "archiveB.txt")))
        self.assertTrue(os.path.exists(os.path.join(scratch_dir, "archiveB.txt")))
        
        self.assertEqual(scratch_dir, archiveB_txt.srcpath)
        self.assertEqual("archiveB.txt", archiveB_txt.relpath)
        self.assertTure(archiveB_txt.exists())
        
        with open(archiveB_txt.abspath, "r") as fd:
            self.assertEqual("This is a GZIP file\n", fd.read())
        
        pass

