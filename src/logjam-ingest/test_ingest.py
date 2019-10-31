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


    def test_match_category(self):
        """ Test that expected categories are matched from file paths """
        # Map sample paths to their "correct" answer
        test_paths = [
            ("scratch_space/950284-vhamemimmgws01-20130713153017-20130713160517/950284/"
             "vhamemimmgws01/20130713153017-20130713160517/mandatory_files/bycast.log", "bycast"),

            ("logjam/scratch_space/950194-vhairoimmsn02-20140520230753-20140520234253/950194/"
             "vhairoimmsn02/20140520230753-20140520234253/mandatory_files/"
             "servermanager.log", "server_manager"),

            ("logjam/scratch_space/950194-vhairoimmsn02-20140520230753-20140520234253/950194/"
             "vhairoimmsn02/20140520230753-20140520234253/system_commands", "system_commands"),

            ("logjam/scratch_space/950194-vhairoimmsn02-20140520215500-20140520231300/950194/"
             "vhairoimmsn02/20140520215500-20140520231300", "other"),

            ("asdf123.log", "other"),

            ("logjam/scratch_space/950166-vhanflimmcn10-20140717025500-20140717040000/950166/"
             "vhanflimmcn10/20140717025500-20140717040000/mandatory_files/messages", "messages"),
            ]

        for path, correct_category in test_paths:
            self.assertEqual(ingest.getCategory(path), correct_category)

    def test_identify_casenum(self):
        """ Test that we can properly identify directories with 10-digit case numbers """

        # Dummy paths based on real inputs. These are not read or written to.
        valid_paths = [
            "/mnt/nfs/storagegrid-01/2004144146",
            "/mnt/nfs/01/2004436294",
            "/mnt/nfs/2004913956",
            ]
        for path in valid_paths:
            case_num = ingest.getCaseNumber(path)
            self.assertNotEqual(case_num, "0", "Case number was zero for valid path %s" % path)

        invalid_paths = [
            "asdfasdf",
            "/",
            "/mnt/nfs/12345",
            ]
        for path in invalid_paths:
            case_num = ingest.getCaseNumber(path)
            self.assertEqual(case_num, "0", "Case number provided for bad folder path %s" % path)

    def test_basic_ingest(self):
        """ Run the full ingest process on a simple set of inputs """
        # Establish paths under the test's temp directory
        input_dir = os.path.join(self.data_dir, "TestInputDir01")
        categ_dir = os.path.join(self.tmp_dir, "categories")
        scratch_dir = os.path.join(self.tmp_dir, "scratch")
        history_dir = os.path.join(self.tmp_dir, "history")

        # Run ingest on sample data
        ingest.ingest_log_files(input_dir, categ_dir, scratch_dir, history_dir)
        
        expected_structure = {
            "2001872931" :
            {
                "lumberjack",
                "servermanager",
                "system_commands"
            },
            
            "2001901245" :
            {
                "bycast.log"
            }
        }   
        
        def is_correct_structure(dir_path, dct):
            ans = True
            
            for entity in os.listdir(dir_path):                     # each entity in dir
                token = next((t for t in dct if t in entity), None) # token within entity
                self.assertTrue(token != None)
                
                full_path = os.path.join(dir_path, entity)
                if os.path.isdir(full_path):
                    ans = is_correct_structure(full_path, dct[token]) and ans
            
            return ans                                              # recurse backwards
        
        self.assertTrue(is_correct_structure(categ_dir, expected_structure))

