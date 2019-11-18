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

import ingest


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

