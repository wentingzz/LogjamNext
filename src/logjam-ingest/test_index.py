"""
@author Wenting Zheng

Tests the features found in the index.py file.
"""


import unittest
import os
import time
import shutil
import tarfile
import stat
import gzip
import subprocess

import index


class IndexDataTestCase(unittest.TestCase):
    """ Tests the indexing functionality """
    
    def test_send_to_es(self):
    
        # TODO: Find a way to test this function!!!
        
        pass


if __name__ == '__main__':
    unittest.main()

