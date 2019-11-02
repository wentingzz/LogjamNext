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


code_src_dir = os.path.dirname(os.path.realpath(__file__))


class TestIngest(unittest.TestCase):
    """
    Test case class for ingest unit tests
    """

    data_dir = os.path.join(code_src_dir, "test-data")

    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(code_src_dir, tmp_name)
        os.mkdir(self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)


    def test_identify_casenum(self):
        """ Tests that we can properly identify directories with 10-digit case numbers """

        # Dummy paths based on real inputs. These are not read or written to.
        valid_paths = [
            "2004144146",
            "2004436294",
            "2004913956",
            ]
        for path in valid_paths:
            case_num = ingest.extract_case_number(path)
            self.assertNotEqual(None, case_num, "Should have found case number %s" % path)

        invalid_paths = [
            "/mnt",
            "/mnt/nfs",
            "/mnt/nfs/2001392039",
            "/2004920192",
            "asdfasdf",
            "/",
            "/mnt/nfs/12345",
            ]
        for path in invalid_paths:
            case_num = ingest.extract_case_number(path)
            self.assertEqual(None, case_num, "Shouldn't have found case number %s" % path)

    def test_basic_ingest(self):
        """ Run the full ingest process on a simple set of inputs """
        # Establish paths under the test's temp directory
        input_dir = os.path.join(self.data_dir, "TestInputDir01")
        categ_dir = os.path.join(self.tmp_dir, "categories")
        scratch_dir = os.path.join(self.tmp_dir, "scratch")
        history_file = os.path.join(self.tmp_dir, "history.txt")
        
        for (basepath, dirs, files) in os.walk(input_dir):          # make files old
            for file in files:
                os.utime(os.path.join(basepath,file), times=(time.time(),0))

        # Run ingest on sample data
        ingest.ingest_log_files(input_dir, scratch_dir, history_file)

        # TODO: Verify ingest worked now that category folders are gone
