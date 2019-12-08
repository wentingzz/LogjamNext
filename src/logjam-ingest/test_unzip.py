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
import zipfile

import unzip
import paths


CODE_SRC_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(CODE_SRC_DIR, "test-data", "Unzip")


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
        unzip.recursive_unzip(os.path.join(self.tmpdir, 'password_7z.7z'), self.tmpdir)

        # Unzip should fail, but it musn't hang.
        if timed_out:
            self.fail("Extracting password-protected 7zip hung for too long")


    def test_gz(self):
        unzip.recursive_unzip(os.path.join(self.tmpdir, 'hello_gz.gz'), self.tmpdir)
        self.assertTrue(os.path.isfile(os.path.join(self.tmpdir, 'hello_gz')))

    def test_corrupt_tgz(self):
        unzip.recursive_unzip(os.path.join(self.tmpdir, 'corrupt.tar.gz'), self.tmpdir)
        # Should have an error but handle gracefully. Output file should not exist.
        self.assertFalse(os.path.exists(os.path.join(self.tmpdir, 'corrupt')))

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

        unzip.recursive_unzip(archive_path, self.tmpdir)

        assert not os.path.isfile(existing_file)

    @classmethod
    def tearDownClass(cls):
        # Remove the compressed data
        shutil.rmtree(cls.tmpdir)


class ExtractZipTestCase(unittest.TestCase):
    """ Tests the function for unzipping zip files only """
    
    def setUp(self):
        tmp_name = "-".join([self._testMethodName, str(int(time.time()))])
        self.tmp_dir = os.path.join(CODE_SRC_DIR, tmp_name)
        os.makedirs(self.tmp_dir)
        self.assertTrue(os.path.isdir(self.tmp_dir))
    
    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        self.assertTrue(not os.path.exists(self.tmp_dir))
    
    def test_basic_zip_files(self):
        dir_to_compress = paths.QuantumEntry(self.tmp_dir, "a/b/c/dir")
        os.makedirs(dir_to_compress.abspath, exist_ok=True)
        with open((dir_to_compress/"fileA.txt").abspath, "w") as fd:
            fd.write("TEXT\n")
        compressed_file = paths.QuantumEntry(self.tmp_dir, "dir.zip")
        decompressed_dir = paths.QuantumEntry(self.tmp_dir, "dir")
        
        shutil.make_archive(
            base_name=os.path.join(self.tmp_dir, "dir"),
            format="zip",
            root_dir=dir_to_compress.abspath)
        self.assertTrue(compressed_file.exists())
        self.assertFalse(decompressed_dir.exists())
        
        # First unzip
        unzip.extract_zip(
            compressed_file,
            paths.QuantumEntry(self.tmp_dir, ""))
        self.assertTrue(compressed_file.exists())
        self.assertTrue(decompressed_dir.exists())
        self.assertTrue((decompressed_dir/"fileA.txt").exists())
        with open((decompressed_dir/"fileA.txt").abspath, "r") as fd:
            self.assertEqual("TEXT\n", fd.read())
        
        # Unzip & already exists
        unzip.extract_zip(
            compressed_file,
            paths.QuantumEntry(self.tmp_dir, ""),
            exist_ok=True)
        self.assertTrue(compressed_file.exists())
        self.assertTrue(decompressed_dir.exists())
        self.assertTrue((decompressed_dir/"fileA.txt").exists())
        with open((decompressed_dir/"fileA.txt").abspath, "r") as fd:
            self.assertEqual("TEXT\n", fd.read())
        
        # Unzip & already exists, but we don't want it to exist so raise exception
        with self.assertRaises(unzip.AcceptableException, msg="Unzip didn't raise exception"):
            unzip.extract_zip(compressed_file,
                paths.QuantumEntry(self.tmp_dir, ""),
                exist_ok=False)
        self.assertTrue(compressed_file.exists())
        self.assertTrue(decompressed_dir.exists())
        self.assertTrue((decompressed_dir/"fileA.txt").exists())
        with open((decompressed_dir/"fileA.txt").abspath, "r") as fd:
            self.assertEqual("TEXT\n", fd.read())

    def test_zip_one_file(self):
        file_to_compress = paths.QuantumEntry(self.tmp_dir, "orig/file.txt")
        os.makedirs(file_to_compress.absdirpath, exist_ok=True)
        with open(file_to_compress.abspath, "w") as fd:
            fd.write("TEXT\n")
        zip_file = paths.QuantumEntry(self.tmp_dir, "file.txt.zip")
        decompressed_file = paths.QuantumEntry(self.tmp_dir, "new/file.txt")
        
        with zipfile.ZipFile(zip_file.abspath, "w") as z:
            z.write(file_to_compress.abspath, arcname="file.txt")
        self.assertTrue(file_to_compress.exists())
        self.assertTrue(file_to_compress.is_file())
        self.assertTrue(zip_file.exists())
        self.assertTrue(zip_file.is_file())
        self.assertFalse(decompressed_file.exists())
        
        # First unzip
        unzip.extract_zip(
            zip_file,
            paths.QuantumEntry(self.tmp_dir, "new"))
        self.assertTrue(zip_file.exists())
        self.assertTrue(zip_file.is_file())
        self.assertTrue(decompressed_file.exists())
        self.assertTrue(decompressed_file.is_file())
        with open(decompressed_file.abspath, "r") as fd:
            self.assertEqual("TEXT\n", fd.read())
        
        # Unzip & already exists
        unzip.extract_zip(
            zip_file,
            paths.QuantumEntry(self.tmp_dir, "new"),
            exist_ok=True)
        self.assertTrue(zip_file.exists())
        self.assertTrue(decompressed_file.exists())
        self.assertTrue(decompressed_file.is_file())
        with open(decompressed_file.abspath, "r") as fd:
            self.assertEqual("TEXT\n", fd.read())
        
        # Unzip & already exists, but we don't want it to exist so raise exception
        with self.assertRaises(unzip.AcceptableException, msg="Unzip didn't raise exception"):
            unzip.extract_zip(
                zip_file,
                paths.QuantumEntry(self.tmp_dir, "new"),
                exist_ok=False)
        self.assertTrue(zip_file.exists())
        self.assertTrue(decompressed_file.exists())
        self.assertTrue(decompressed_file.is_file())
        with open(decompressed_file.abspath, "r") as fd:
            self.assertEqual("TEXT\n", fd.read())

    def test_corrupt_zip(self):
        zip_file = paths.QuantumEntry(self.tmp_dir, "dir.zip")
        decompressed_dir = paths.QuantumEntry(self.tmp_dir, "dir")
        
        with open(zip_file.abspath, "wb") as fd:
            fd.write(b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF")
        self.assertTrue(zip_file.exists())
        self.assertTrue(zip_file.is_file())
        self.assertFalse(decompressed_dir.exists())
        
        with self.assertRaises(unzip.AcceptableException, msg="Unzip didn't throw on corrupt"):
            unzip.extract_zip(
                zip_file,
                paths.QuantumEntry(self.tmp_dir, ""))
        self.assertTrue(zip_file.exists())
        self.assertTrue(zip_file.is_file())
        self.assertFalse(decompressed_dir.exists())


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

