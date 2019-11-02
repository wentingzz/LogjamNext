"""
@author Wenting Zheng
@author Nathaniel Brooks

Tests the features found in the fields.py file.
"""


import unittest
import os
import time
import shutil
import tarfile
import stat
import gzip
import subprocess

import fields


code_src_dir = os.path.dirname(os.path.realpath(__file__))


class ExtractFieldsTestCase(unittest.TestCase):
    """ Test case for extracting different kinds of fields """
    
    data_dir = os.path.join(code_src_dir, "test-data")

    def test_get_category(self):
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
            self.assertEqual(fields.get_category(path), correct_category)

    def test_get_case_number(self):
        """ Tests that we can properly identify directories with 10-digit case numbers """

        # Dummy paths based on real inputs. These are not read or written to.
        valid_paths = [
            "2004144146",
            "2004436294",
            "2004913956",
            ]
        for path in valid_paths:
            case_num = fields.get_case_number(path)
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
            case_num = fields.get_case_number(path)
            self.assertEqual(None, case_num, "Shouldn't have found case number %s" % path)

    def test_get_version(self):
        test_data_dir = os.path.join(self.data_dir, "StandardFiles")
    
        try:
            version = fields.get_storage_grid_version(os.path.join(test_data_dir,'1234567890', 'system_commands.txt'))
            self.assertEqual(version, 'unknown')
        except Exception as exc:
            self.fail(exc)

        try:
            version = fields.get_storage_grid_version(os.path.join(test_data_dir,'123', 'system_commands'))
            self.assertEqual(version, '100.100.100-12345678.0224.asdfg12345')
        except Exception as exc:
            self.fail(exc)

        try:
            version = fields.get_storage_grid_version(os.path.join(test_data_dir,'null', 'system_commands.txt'))
            self.assertEqual(version, 'unknown')
        except Exception as exc:
            self.fail(exc)

