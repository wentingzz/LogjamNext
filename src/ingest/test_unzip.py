"""
@author Wenting Zheng
@author Jeremy Schmidt
@author Nathaniel Brooks

Tests the utility unzipping function and its helper functions.
"""


import unittest
import os
import shutil
import signal
import time
import tarfile
import stat
import gzip
import subprocess

import unzip


class RecursiveUnzipTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        dirname = os.path.dirname(os.path.realpath(__file__))
        cls.tmpdir = os.path.join(dirname, "test-" + str(int(time.time())))
        cls.srcdir = os.path.join(dirname, "test-data", "Unzip")
        os.mkdir(cls.tmpdir)
        
        shutil.make_archive(os.path.join(cls.tmpdir, 'hello_zip'), 'zip', os.path.join(cls.srcdir, 'hello_zip'))
        shutil.make_archive(os.path.join(cls.tmpdir, 'hello_tar'), 'tar', os.path.join(cls.srcdir, 'hello_tar'))
        shutil.copy2(os.path.join(cls.tmpdir, 'hello_zip.zip'), os.path.join(cls.srcdir, 'hello_targz'))
        shutil.make_archive(os.path.join(cls.tmpdir, 'hello_targz'), 'gztar', os.path.join(cls.srcdir, 'hello_targz'))
        with gzip.open(os.path.join(cls.tmpdir, 'hello_gz.gz'), 'wb') as f:
            f.write('hello_gz.txt'.encode())

        process = subprocess.call(['7z', 'a', os.path.join(cls.tmpdir, 'hello_7z.7z'), os.path.join(cls.srcdir, 'hello_7z', '7z.txt'), '-mx9'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        # Prepare compressed data for each test gz tgz
        # (zip, gz, tar.gz, 7z, tar) plus multiple layer versions

        # Preapare a password-protected 7z
        subprocess.call(['7z', 'a', os.path.join(cls.tmpdir, 'password_7z.7z'), os.path.join(cls.srcdir, 'password_7z', 'password_7z.txt'), '-mx9', "-pPassword123"])


        # Prepare a corrupt tar.gz
        with open(os.path.join(cls.tmpdir, "corrupt.tar.gz"), "wb") as corrupt_file:
            for _ in range(1000):
                corrupt_file.write(os.urandom(1000))

    def test_targz(self):
        # Call recursive unzip
        unzip.recursive_unzip(os.path.join(self.tmpdir, 'hello_targz.tar.gz'), self.tmpdir)
        self.assertTrue(os.path.isdir(os.path.join(self.tmpdir, 'hello_targz')))
        self.assertTrue(os.path.isdir(os.path.join(self.tmpdir, 'hello_targz', 'folder')))
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_targz', 'folder', 'targz.txt')))
        self.assertTrue(os.path.isdir(os.path.join(self.tmpdir, 'hello_targz', 'hello_zip')))
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_targz', 'hello_zip', 'zip.txt')))

    def test_tar(self):
        unzip.recursive_unzip(os.path.join(self.tmpdir, 'hello_tar.tar'), self.tmpdir)
        self.assertTrue(os.path.isdir(os.path.join(self.tmpdir, 'hello_tar')))
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_tar', 'tar.txt')))

    def test_zip(self):
        src_file = os.path.join(self.tmpdir, "hello_zip.zip")
        dest_dir = self.tmpdir
        dest_unzipped_dir = os.path.join(dest_dir, "hello_zip")
        if os.path.exists(dest_unzipped_dir):
            shutil.rmtree(dest_unzipped_dir)
        
        unzip.recursive_unzip(src_file, dest_dir)
        self.assertTrue(os.path.isdir(dest_dir))
        self.assertTrue(os.path.isdir(dest_unzipped_dir))
        self.assertTrue(os.path.isfile(os.path.join(dest_unzipped_dir, 'zip.txt')))
    
    def test_zip_deep_path(self):
        src_file = os.path.join(self.tmpdir, "hello_zip.zip")
        dest_dir = os.path.join(self.tmpdir, "dirA", "dirB", "dirC")
        dest_unzipped_dir = os.path.join(dest_dir, "hello_zip")
        if os.path.exists(dest_unzipped_dir):
            shutil.rmtree(dest_unzipped_dir)
        
        unzip.recursive_unzip(src_file, dest_dir)
        self.assertTrue(os.path.isdir(dest_dir))
        self.assertTrue(os.path.isdir(dest_unzipped_dir))
        self.assertTrue(os.path.isfile(os.path.join(dest_unzipped_dir, "zip.txt")))
    
    def test_7z(self):
        unzip.recursive_unzip(os.path.join(self.tmpdir, 'hello_7z.7z'), self.tmpdir)
        self.assertTrue(os.path.isdir(os.path.join(self.tmpdir, 'hello_7z')))
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_7z', '7z.txt')))

    def test_password_7z(self):
        timed_out = False
        def timeout_handler(signum, frame):
            nonlocal timed_out  # Pull flag from outer scope
            timed_out = True
            raise Exception("Received timeout signal")

        signal.signal(signal.SIGALRM, timeout_handler)

        signal.alarm(10)  # Send timeout signal in 10 seconds
        try:
            unzip.recursive_unzip(os.path.join(self.tmpdir, 'password_7z.7z'), self.tmpdir)
            # Unzip should fail, but it musn't hang.
            if timed_out:
                self.fail("Extracting password-protected 7zip hung for too long")
            else:
                self.fail("Extracting password-protected 7zip should fail")
        except unzip.AcceptableException:
            pass
        

    def test_gz(self):
        unzip.recursive_unzip(os.path.join(self.tmpdir, 'hello_gz.gz'), self.tmpdir)
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_gz')))

    def test_corrupt_tgz(self):
        # Should fail and raise AcceptableException
        try:
            unzip.recursive_unzip(os.path.join(self.tmpdir, 'corrupt.tar.gz'), self.tmpdir)
            self.fail("Unzipping corrupt tgz should fail")
        except unzip.AcceptableException:
            pass

    def test_folder_exists(self):
        """
        Folder exists. Zip it. Remove file inside folder. Unzip.
        Unzipping should be skipped and file should not be present.
        """
        existing_path = os.path.join(self.tmpdir, "existing_folder")
        os.mkdir(existing_path)

        existing_file = os.path.join(existing_path, "file1.txt")
        with open(existing_file, "w") as filep:
            filep.write("hello")

        assert os.path.isfile(existing_file)

        shutil.make_archive(existing_path, "zip", existing_path)
        archive_path = os.path.join(self.tmpdir, "existing_folder.zip")

        assert os.path.isfile(archive_path)

        os.remove(existing_file)
        assert not os.path.isfile(existing_file)

        try:
            unzip.recursive_unzip(archive_path, self.tmpdir)
            self.fail("Unzipping should be skipped")
        except unzip.AcceptableException:
            assert not os.path.isfile(existing_file)

    @classmethod
    def tearDownClass(cls):
        # Remove the compressed data
        shutil.rmtree(cls.tmpdir)


class DeleteFileTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dirname = os.path.dirname(os.path.realpath(__file__))
        cls.tmpdir = os.path.join(dirname, "test-" + str(int(time.time())))
        os.mkdir(cls.tmpdir)

    def test_invalidFile(self):
        try:
            unzip.delete_file(self.tmpdir)
            self.fail("unzip.delete_file deletes a directory")
        except Exception as exc:
            pass
        
        try:
            unzip.delete_file(os.path.join(self.tmpdir, 'invalid_path'))
            self.fail(os.path.join(self.tmpdir, 'invalid_path') + " should not exist")
        except Exception as exc:
            pass
        
        
        file = open(os.path.join(self.tmpdir, "copy.txt"), "w")
        file.write("Hello World")
        file.close()
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'copy.txt')))
        os.chmod(os.path.join(self.tmpdir, 'copy.txt'), stat.S_IRUSR)
        unzip.delete_file(os.path.join(self.tmpdir, 'copy.txt'))
        self.assertFalse(os.path.exists(os.path.join(self.tmpdir, 'copy.txt')))
            
    @classmethod
    def tearDownClass(cls):
        # Remove the compressed data
        shutil.rmtree(cls.tmpdir)


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
        unzip.delete_directory(self.tmpsubdir)
        self.assertFalse(os.access(os.path.join(self.tmpdir, "copy.txt"), os.W_OK))

    @classmethod
    def tearDownClass(cls):
        # Remove the compressed data
        shutil.rmtree(cls.tmpdir)
        pass


class ExtensionStrippingTestCase(unittest.TestCase):
    """ Tests the zip extension strippping functions """
    
    def test_strip_all_zip_exts(self):
        self.assertEqual("/f", unzip.strip_all_zip_exts("/f.tgz.tar.zip.zip.gz"))
        self.assertEqual("b.png", unzip.strip_all_zip_exts("b.png.zip.7z.gz"))
        self.assertEqual(".git.ziip", unzip.strip_all_zip_exts(".git.ziip.zip"))
    
    def test_strip_zip_ext(self):
        self.assertEqual("/dir/dir/img.jpg", unzip.strip_zip_ext("/dir/dir/img.jpg.zip"))
        self.assertEqual("/.git", unzip.strip_zip_ext("/.git"))
        self.assertEqual("./pack.tar.gz", unzip.strip_zip_ext("./pack.tar.gz.zip"))
        self.assertEqual("./f", unzip.strip_zip_ext("./f.7z"))
        self.assertEqual("./x.tar", unzip.strip_zip_ext("./x.tar.tgz"))


if __name__ == '__main__':
    unittest.main()

