import unittest
import os
import time
import shutil
import tarfile
import stat
import gzip
import subprocess

import utils


# coverage report: 72-74, 85-87, 104-106, 112-113, 
class RecursiveUnzipTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        dirname = os.path.dirname(os.path.realpath(__file__))
        cls.tmpdir = os.path.join(dirname, "test-" + str(int(time.time())))
        cls.srcdir = os.path.join(dirname, "test-data", "Utils")
        os.mkdir(cls.tmpdir)
        
        shutil.make_archive(os.path.join(cls.tmpdir, 'hello_zip'), 'zip', os.path.join(cls.srcdir, 'hello_zip'))
        shutil.make_archive(os.path.join(cls.tmpdir, 'hello_tar'), 'tar', os.path.join(cls.srcdir, 'hello_tar'))
        shutil.copy2(os.path.join(cls.tmpdir, 'hello_zip.zip'), os.path.join(cls.srcdir, 'hello_targz'))
        shutil.make_archive(os.path.join(cls.tmpdir, 'hello_targz'), 'gztar', os.path.join(cls.srcdir, 'hello_targz'))
        with gzip.open(os.path.join(cls.tmpdir, 'hello_gz.gz'), 'wb') as f:
            f.write('hello_gz.txt')
        process = subprocess.call(['7z', 'a', os.path.join(cls.tmpdir, 'hello_7z.7z'), os.path.join(cls.srcdir, 'hello_7z', '7z.txt'), '-mx9'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        # Prepare compressed data for each test gz tgz
        # (zip, gz, tar.gz, 7z, tar) plus multiple layer versions

    def test_targz(self):
        # Call recursive unzip
        utils.recursive_unzip(os.path.join(self.tmpdir, 'hello_targz.tar.gz'), self.tmpdir)
        self.assertTrue(os.path.isdir(os.path.join(self.tmpdir, 'hello_targz')))
        self.assertTrue(os.path.isdir(os.path.join(self.tmpdir, 'hello_targz', 'folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_targz', 'folder', 'targz.txt')))
        self.assertTrue(os.path.isdir(os.path.join(self.tmpdir, 'hello_targz', 'hello_zip')))
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_targz', 'hello_zip', 'zip.txt')))


    def test_tar(self):
        utils.recursive_unzip(os.path.join(self.tmpdir, 'hello_tar.tar'), self.tmpdir)
        self.assertTrue(os.path.isdir(os.path.join(self.tmpdir, 'hello_tar')))
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_tar', 'tar.txt')))

    def test_zip(self):
        utils.recursive_unzip(os.path.join(self.tmpdir, 'hello_zip.zip'), self.tmpdir)
        self.assertTrue(os.path.isdir(os.path.join(self.tmpdir, 'hello_zip')))
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_zip', 'zip.txt')))
    
    def test_7z(self):
        utils.recursive_unzip(os.path.join(self.tmpdir, 'hello_7z.7z'), self.tmpdir)
        self.assertTrue(os.path.isdir(os.path.join(self.tmpdir, 'hello_7z')))
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_7z', '7z.txt')))

    def test_gz(self):
        utils.recursive_unzip(os.path.join(self.tmpdir, 'hello_gz.gz'), self.tmpdir)
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_gz')))

    @classmethod
    def tearDownClass(cls):
        # Remove the compressed data
        shutil.rmtree(cls.tmpdir)



# coverage report: 150-170

class DeleteFileTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dirname = os.path.dirname(os.path.realpath(__file__))
        cls.tmpdir = os.path.join(dirname, "test-" + str(int(time.time())))
        os.mkdir(cls.tmpdir)

    def test_invalidFile(self):
        try:
            utils.delete_file(self.tmpdir)
            self.fail("utils.delete_file deletes a directory")
        except Exception as exc:
            pass
        
        try:
            utils.delete_file(os.path.join(self.tmpdir, 'invalid_path'))
            self.fail(os.path.join(self.tmpdir, 'invalid_path') + " should not exist")
        except Exception as exc:
            pass
        
        
        file = open(os.path.join(self.tmpdir, "copy.txt"), "w")
        file.write("Hello World")
        file.close()
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'copy.txt')))
        os.chmod(os.path.join(self.tmpdir, 'copy.txt'), stat.S_IRUSR)
        utils.delete_file(os.path.join(self.tmpdir, 'copy.txt'))
        self.assertFalse(os.path.exists(os.path.join(self.tmpdir, 'copy.txt')))
            
    @classmethod
    def tearDownClass(cls):
        # Remove the compressed data
        shutil.rmtree(cls.tmpdir)

# coverage report: 188-203
class DeleteDirectoryTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dirname = os.path.dirname(os.path.realpath(__file__))
        cls.tmpdir = os.path.join(dirname, "test-" + str(int(time.time())))
        cls.tmpsubdir = os.path.join(cls.tmpdir, "temp")
        os.mkdir(cls.tmpdir)
        os.mkdir(cls.tmpsubdir)

    def test_invalidDirectory(self):
        os.chmod(self.tmpsubdir, stat.S_IREAD)
        utils.delete_directory(self.tmpsubdir)
        self.assertFalse(os.access(os.path.join(self.tmpdir, "copy.txt"), os.W_OK))

    @classmethod
    def tearDownClass(cls):
        # Remove the compressed data
        shutil.rmtree(cls.tmpdir)
        pass

if __name__ == '__main__':
    unittest.main()
