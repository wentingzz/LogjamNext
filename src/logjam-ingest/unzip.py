"""
@author Renata Ann Zeitler
@author Josh Good
@author Jeremy Schmidt
@author Nathaniel Brooks

This script exposes a function that recursively unzips deeply nested
directories into a specified file system location as well as additional
helper functions.
"""


import os
import types
import time
import platform
import logging
import shutil
import stat
import conans
import gzip
import patoolib
import subprocess
import zipfile

import paths
import patoolib_patch
patoolib_patch.patch_7z(patoolib)


SUPPORTED_FILE_TYPES = {".gz", ".tgz", ".tar", ".zip", ".7z"}


class AcceptableException(Exception):
    def __init___(self, arg):
        Exception.__init__(self, arg)


def recursive_unzip(src, dest, action=lambda file_abspath: None):
    """
    Recursively unzips deeply nested directories into a provided location.
    The original zip file will not be deleted. The fully unzipped directory will have
    no compressed files. If compressed files are encountered, they are unzipped in their
    respective locations and the temporary archive/zip file is deleted.
    src : string
        path to source directory file to unzip
    dest : string
        path to destination directory to place unzipped files
    action : function(file_abspath) -> return None
        action function to take on each file extracted
    return : string
        path to fully unzipped directory
    """
    assert os.path.exists(src), "Source does not exist: "+src
    assert os.path.isfile(src), "Source should be a file: "+src
    assert os.path.splitext(src)[1] in SUPPORTED_FILE_TYPES, "Invalid extension: "+src
    src = os.path.abspath(src)
    dest = os.path.abspath(dest)
    os.makedirs(dest, exist_ok=True)

    # Capture the modified time of the archive to force it upon its contents
    try:
        archive_mtime = os.path.getmtime(src)
    except:
        logging.warning("Get mod time failed, skipping zipped file: %s", src)
        # Do not change permissions, may not own file
        raise AcceptableException("Get mod time failed")      # raise exception
    
    def handle_extracted_file(path):
        """ Callback for each unzipped file """
        path = os.path.abspath(path)
        
        if not try_fs_operation(path, lambda p: os.utime(p, (time.time(), archive_mtime))):
            logging.warning("Set mod time failed, skipping file: %s", path)
            delete_file(path)
            raise AcceptableException("Set mod time failed")
        
        if os.path.splitext(path)[1] in SUPPORTED_FILE_TYPES:
            recursive_unzip(path, os.path.dirname(path), action)
            delete_file(path)                   # delete zip file, unzipped same location
        else:
            action(path)                        # basic file, perform action
            #delete_file(path)                  # no basic file clean up, leave for caller
        return
    
    extension = os.path.splitext(src)[1]        # dest file/dir will mirror old name
    dest = os.path.join(dest, strip_all_zip_exts(os.path.basename(src)))
    assert os.path.isabs(dest), "New destination path not absolute: "+dest
    
    if os.path.exists(dest):                    # file/dir already exists
        logging.warning("This path was already unzipped: %s", dest)
        raise AcceptableException("This path was already unzipped")
    
    if extension == ".zip":
        logging.debug("Unzipping: %s", src)
        
        zip_file = src
        dest_dir = os.path.dirname(dest)
        unzip_entry = paths.QuantumEntry(dest_dir, strip_zip_ext(os.path.basename(src)))
        assert unzip_entry.abspath == os.path.abspath(dest)
        
        try:
            extract_zip(
                paths.QuantumEntry(os.path.dirname(zip_file), os.path.basename(zip_file)),
                paths.QuantumEntry(os.path.dirname(dest_dir), os.path.basename(dest_dir)),
                exist_ok=True)
            assert unzip_entry.exists()
        
        except AcceptableException as e:
            logging.critical("Error during ZipFile unzip: %s", e)
            if unzip_entry.exists():
                unzip_entry.delete()
            raise AcceptableException("Error during ZipFile unzip: %s", e)
        
        if unzip_entry.is_dir():
            recursive_walk(unzip_entry.abspath, handle_extracted_file)
        elif unzip_entry.is_file():
            handle_extracted_file(unzip_entry.abspath)
        else:
            logging.critical("This execution path should never be reached")
            raise Exception("Seemingly impossible execution path")
    
    elif extension == ".tar" or extension == ".tgz": 
        logging.debug("Unzipping: %s", src)
        
        assert not os.path.exists(dest), "Directory should not already exist: "+dest
        os.makedirs(dest)                       # make dir to unpack file contents
        
        error_flag = False
        try:                                    # exception handling here only
            conans.tools.unzip(src, dest, keep_permissions=False)
        except Exception as e:
            logging.critical("Error during Conan unzip: %s", e)
            error_flag = True                   # just log it and skip it
        
        if not error_flag:
            recursive_walk(dest, handle_extracted_file)# walk & unzip if need be
            #delete_directory(dest)             # no basic dir clean up, leave for caller
        else:
            if os.path.exists(dest):
                delete_directory(dest)
            raise AcceptableException("Error during Conan unzip")
        
    elif extension == ".gz":
        logging.debug("Decompressing: %s", src)
        
        error_flag = False
        try:                                    # exception handling here only
            with gzip.open(src, "rb") as in_fd, open(dest, "wb") as out_fd:
                while True:
                    data = in_fd.read(1000000)
                    if data == b'' or data == None or not data:
                        break
                    out_fd.write(data)
        except Exception as e:
            logging.critical("Error during GZip unzip: %s", e)
            error_flag = True                   # just log it and skip it
        
        if not error_flag:
            handle_extracted_file(dest) # Recurse for an archive, perform 'action' for a regular file
        else:
            if os.path.exists(dest):
                if os.path.isdir(dest):
                    delete_directory(dest)
                else:
                    delete_file(dest)
            raise AcceptableException("Error during GZip unzip")
    
    elif extension == ".7z":
        logging.debug("7z Decompressing: %s", src)
        
        assert not os.path.exists(dest), "Directory should not already exist: "+dest
        os.makedirs(dest)                       # make dir to unpack file contents
        
        error_flag = False
        try:                                    # exception handling here only
            patoolib.extract_archive(src, outdir=dest)
        except Exception as e:
            logging.critical("Error during patool 7zip extraction: %s", e)
            error_flag = True                   # just log it and skip it
        
        if not error_flag:
            recursive_walk(dest, handle_extracted_file)# walk & unzip if need be
            #delete_directory(dest)             # no basic dir clean up, leave for caller
        else:
            if os.path.exists(dest):
                delete_directory(dest)
            raise AcceptableException("Error during patool 7zip extraction")
    
    else:
        logging.critical("This execution path should never be reached")
        raise Exception("Seemingly impossible execution path")
    
    return


def recursive_walk(src, action):
    """
    Recursively walks deeply nested directories performing actions on each file.
    src : string
        path to source directory
    action : function(file_abspath) -> return None
        action function to take on each file
    """
    assert os.path.exists(src), "Source does not exist: "+src
    assert os.path.isdir(src), "Source should be a dir: "+src
    assert os.path.isabs(src), "Source path should be absolute: "+src
    assert type(action) in [types.FunctionType, types.LambdaType], "Parameter action was not a function"
    
    for (dirpath,dirnames,filenames) in os.walk(src):
        for file in filenames:
            file_abspath = os.path.join(dirpath,file)
            assert os.path.isabs(file_abspath)
            action(file_abspath)
    
    return


def lift_permissions(path):
    """
    Recursively chmods input file to 755
    """
    if platform.system() != "Windows":
        parent_dir = os.path.dirname(path)
        exit_code = os.system("chmod -R 755 {}".format(parent_dir))
        if exit_code != 0:
            logging.warning("Bad exit code for chmod: %d %s", exit_code, path)
    else:
        os.chmod(path, stat.S_IWRITE)   # turn off read-only


def try_fs_operation(path, func):
    """
    Tries to execute the given file system operation with lift_permissions
    as a backup in case a 'Permission Denied' error is raised.
    """
    try:
        func(path)                      # try operation with the given path
        return True                     # operation succeeded, return True
    except OSError as e:
        if e.errno != 13:               # not permission denied (unknown error code)
            return False                # could not complete operation, return failure
        
    lift_permissions(path)              # try raising the permissions of the path
    
    try:
        func(path)                      # try operation again
        return True                     # operation succeeded on 2nd try, return True
    except:
        return False                    # error occurred, couldn't fix it


def extract_zip(zip_file, dest_dir, *, exist_ok=True):
    """
    Unzips the provided zip file into the destination directory. Assumes
    that Logjam does not own the zip file. If the zip file unzips into a single
    file, then that is placed under dest_dir. Otherwise, place the unzipped contents
    into a directory named after the filename portion of the zip file. Guarantees that
    there is always a directory or file in the dest_dir named after the zip_file (makes
    this function idempotent). Errors during unzipping are propagated through exceptions.
    """
    assert isinstance(zip_file, paths.QuantumEntry), "zip_file was not type QuantumEntry"
    assert isinstance(dest_dir, paths.QuantumEntry), "dest_dir was not type QuantumEntry"
    assert zip_file.extension == ".zip", "zip_file had no .zip ext: " + zip_file.abspath
    
    os.makedirs(dest_dir.abspath, exist_ok=True)
    assert dest_dir.is_dir(), "dest_dir was not a directory: " + dest_dir.abspath
    
    base_name = zip_file.filename                               # ex. d.tar.zip -> d.tar
    unzip_dir = dest_dir/base_name                              # ex. /tmp/d.tar
    if unzip_dir.exists():
        if exist_ok:
            return True
        else:
            raise AcceptableException("Already unzipped!")
    
    os.makedirs(unzip_dir.abspath, exist_ok=True)
    assert unzip_dir.is_dir(), "unzip_dir was not a directory: " + unzip_dir.abspath
    
    if True:                                                    # select faster branch
        try:
            with zipfile.ZipFile(zip_file.abspath, "r") as z:
                z.extractall(path=unzip_dir.abspath)
        except zipfile.BadZipFile as e:
            raise AcceptableException("Python 3 ZipFile failed, exception: %s" % str(e))
    else:
        ans = subprocess.run(["unzip", "-q", zip_file.abspath, "-d", unzip_dir.abspath])
        if ans.returncode != 0:
            raise AcceptableException("CLI unzip failed, return code: %d" % ans.returncode)
    assert zip_file.exists()
    assert unzip_dir.exists()
    
    children = os.listdir(unzip_dir.abspath)
    if len(children)==1 and children[0]==base_name:             # ex. /tmp/d.tar/d.tar
        unzip_file_orig = unzip_dir/children[0]
        unzip_file_tmp = dest_dir/(children[0]+".zip.zip.zip")
        shutil.move(unzip_file_orig.abspath, unzip_file_tmp.abspath)
        assert not unzip_file_orig.exists()
        assert unzip_file_tmp.exists()
        
        shutil.rmtree(unzip_dir.abspath)
        assert not unzip_dir.exists()
        
        unzip_file_new = dest_dir/children[0]
        shutil.move(unzip_file_tmp.abspath, unzip_file_new.abspath)
        assert not unzip_file_tmp.exists()
        assert unzip_file_new.exists()
        return True
        
    else:
        return True


def delete_file(path):
    """
    Attempts to delete a file. If there is a problem halt the program.
    path : string
        path of the file to delete
    """
    path = os.path.abspath(path)
    
    if not try_fs_operation(path, lambda p: os.remove(p)):
        logging.critical("File deletion failed, skipping file: %s", path)
        return False
    
    return True


def delete_directory(path):
    """
    Attempts to delete a directory. If there is a problem halt the program.
    path : string
        path of the directory to delete
    """
    path = os.path.abspath(path)
    
    if not try_fs_operation(path, lambda p: shutil.rmtree(p)):
        logging.critical("Directory deletion failed, skipping directory; %s", path)
        return False
    
    return True


def strip_all_zip_exts(path):
    """
    Strips all the zip extensions from the path and returns the new path without all
    the zip extensions. If the path did not have zip extensions, returns it unchanged.
    """
    while True:
        new_path = strip_zip_ext(path)              # strip one extension
        if new_path == path:                        # was extension stripped?
            return path                             # no change, all done!
        path = new_path                             # recurse down path


def strip_zip_ext(path):
    """
    Strips a zip extension off the provided path and returns the new path without
    the extension. If the path does not have a zip extension, returns the same path.
    """
    (prior, extension) = os.path.splitext(path)
    if extension in SUPPORTED_FILE_TYPES:
        return prior
    else:
        return path

