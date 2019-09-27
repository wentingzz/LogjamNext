"""
Unit tests for top-level ingestion script
@author Jeremy Schmidt
"""
import os
import shutil
import time
import unittest
import sqlite3


from logjam import ingest


class TestIngest(unittest.TestCase):
    """
    Test case class for ingest unit tests
    """

    @classmethod
    def setUpClass(cls):
        dirname = os.path.dirname(os.path.realpath(__file__))
        cls.tmpdir = os.path.join(dirname, "test-" + str(int(time.time())))
        os.mkdir(cls.tmpdir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir)

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

    def test_init_db(self):
        """ Test that db and table are created """
        db_name = os.path.join(self.tmpdir, 'test.db')
        ingest.initDatabase(db_name)
        self.assertTrue(os.path.exists(db_name))

        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Make sure the table exists by querying it
        cursor.execute("SELECT * from paths")
