"""
Unit tests for top-level ingestion script
@author Jeremy Schmidt
"""
import os

from pyfakefs.fake_filesystem_unittest import TestCase
from logjam import ingest


class TestIngest(TestCase):
    """
    Test case class for ingest unit tests
    """
    dirname = os.path.dirname(os.path.realpath(__file__))
    data_dir = os.path.join(dirname, "data")

    def setUp(self):
        self.setUpPyfakefs()
        # make the file accessible in the fake file system
        self.fs.add_real_directory(self.data_dir)

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
