import unittest
import os
import time
import shutil
import tarfile
import stat
import gzip
import subprocess

import index


class ProcessFilesTestCase(unittest.TestCase):

    
    @classmethod
    def setUpClass(cls):
        dirname = os.path.dirname(os.path.realpath(__file__))
        data_dir = os.path.join(dirname, "test-data")
        cls.datadir = os.path.join(dirname, "test-data", 'StandardFiles')
        cls.tmpdir = os.path.join(dirname, "test-index-" + str(int(time.time())))
        os.mkdir(cls.tmpdir)

    def test_get_version(self):
        try:
            version = index.get_storage_grid_version(os.path.join(self.datadir,'1234567890', 'system_commands.txt'))
            self.assertEqual(version, 'unknown')
        except Exception as exc:
            self.fail(exc)
        
        try:
            version = index.get_storage_grid_version(os.path.join(self.datadir,'123', 'system_commands'))
            self.assertEqual(version, '100.100.100-12345678.0224.asdfg12345')
        except Exception as exc:
            self.fail(exc)
            
        try:
            version = index.get_storage_grid_version(os.path.join(self.datadir,'null', 'system_commands.txt'))
            self.assertEqual(version, 'unknown')
        except Exception as exc:
            self.fail(exc)

    def test_is_storagegrid(self):
        try:
            self.assertTrue(index.is_storagegrid(os.path.join(self.datadir,'1234567890', 'bycast.log')))
        except Exception as exc:
            self.fail(exc)
        
        try:
            self.assertTrue(index.is_storagegrid(os.path.join(self.datadir,'1234567890', 'bycast.log')))
        except Exception as exc:
            self.fail(exc)
            
        try:
            self.assertFalse(index.is_storagegrid(os.path.join(self.datadir,'1234567890', 'system_commands.txt')))
        except Exception as exc:
            self.fail(exc)
        
        try:
            self.assertTrue(index.is_storagegrid(os.path.join(self.datadir,'1234567890', 'bycast.log', 'bycast.log')))
        except Exception as exc:
            self.fail(exc)
        
            
    def test_process_files(self):
        try:
            files = index.process_files_in_node(os.path.join(self.datadir, '123'), [])
            self.assertEqual(3, len(files))
            self.assertTrue(next((True for f in files if "system_commands" in f), False))
            self.assertTrue(next((True for f in files if "lumberjack.log" in f), False))
            self.assertTrue(next((True for f in files if "servermanager.log" in f), False))
        except Exception as exc:
            self.fail(exc)

    @classmethod
    def tearDownClass(cls):
        # Remove the compressed data
        shutil.rmtree(cls.tmpdir)



# coverage report: 150-170

class StashFilesTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dirname = os.path.dirname(os.path.realpath(__file__))
        data_dir = os.path.join(dirname, "test-data")
        cls.datadir = os.path.join(dirname, "test-data", 'StandardFiles')
        cls.tmpdir = os.path.join(dirname, "test-index-" + str(int(time.time())))

    def test_stash_node(self):
        try:
            index.stash_node_in_elk(self.datadir, None, self.tmpdir, False)
            self.fail(exc)
        except Exception as exc:
            pass
        
        try:
            if os.path.exists(self.tmpdir):
                index.stash_node_in_elk(os.path.join(self.datadir,'1234567890'), None)
            else:
                index.stash_node_in_elk(os.path.join(self.datadir,'1234567890'), None)
            self.fail(exc)
        except Exception as exc:
            pass
        
        try:
            index.stash_node_in_elk(os.path.join(self.datadir,'123'), '123')
        except Exception as exc:
            self.fail(exc)
        
            
    def test_files_node(self):
        try:
            index.stash_file_in_elk(self.datadir, 'txt', None)
            self.fail(exc)
        except Exception as exc:
            pass
        
        try:
            index.stash_file_in_elk(os.path.join(self.datadir,'1234567890'), 'txt', None)
            self.fail(exc)
        except Exception as exc:
            pass
        
        try:
            index.stash_file_in_elk(os.path.join(self.datadir,'123', 'system_commands'), 'system_commands', '123')
        except Exception as exc:
            self.fail(exc)
       
    @classmethod
    def tearDownClass(cls):
        # Remove the compressed data
        shutil.rmtree(cls.tmpdir)

if __name__ == '__main__':
    unittest.main()
