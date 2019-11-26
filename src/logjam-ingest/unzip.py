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

import patoolib_patch
patoolib_patch.patch_7z(patoolib)


SUPPORTED_FILE_TYPES = {".gz", ".tgz", ".tar", ".zip", ".7z"}


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
        pass                                    # no change permissions, may not own file
        return                                  # just kill function, don't unzip
    
    def handle_extracted_file(path):
        """ Callback for each unzipped file """
        path = os.path.abspath(path)
        
        if not try_fs_operation(path, lambda p: os.utime(p, (time.time(), archive_mtime))):
            logging.warning("Set mod time failed, skipping file: %s", path)
            delete_file(path)
            return                              
        
        if os.path.splitext(path)[1] in SUPPORTED_FILE_TYPES:
            recursive_unzip(path, os.path.dirname(path), action)
            delete_file(path)                   # delete zip file, unzipped same location
        else:
            action(path)                        # basic file, perform action
            #delete_file(path)                  # no basic file clean up, leave for caller
        
        return
    
    extension = os.path.splitext(src)[1]        # dest file/dir will mirror old name
    dest = os.path.join(dest, strip_zip_ext(os.path.basename(src)))
    assert os.path.isabs(dest), "New destination path not absolute: "+dest
    
    if os.path.exists(dest):                    # file/dir already exists
        logging.warning("This path was already unzipped: %s", dest)
        return                                  # skip the unzip step
    
    if extension == ".zip" or extension == ".tar" or extension == ".tgz": 
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
        elif os.path.exists(dest):
            delete_directory(dest)
        
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
        elif os.path.exists(dest):
            if os.path.isdir(dest):
                delete_directory(dest)
            else:
                delete_file(dest)
    
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
        elif os.path.exists(dest):
            delete_directory(dest)
    
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
