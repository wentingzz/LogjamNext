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


class ExtractFieldsTestCase(unittest.TestCase):
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

