"""
@author Renata Ann Zeitler - Original authors of unzipping function
@author Josh Good - Original authors of unzipping function
@author Jeremy Schmidt - Updated to Python 3 2019-09-09
@author Nathaniel Brooks - Treat unzipping as read-only 2019-09-08

This script exposes a function that recursively unzips deeply nested
directories into a specified file system location.
"""


import os
import types
import platform
import logging
import shutil
import stat
import conans
import gzip
import pyunpack


recursive_unzip_file_types = {".gz", ".tgz", ".tar", ".zip", ".7z"}


def recursive_unzip(src, dest, action=lambda file_abspath: None):
    ''' Recursively unzips deeply nested directories into a provided location.
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
    '''
    assert os.path.exists(src), "Source does not exist: "+src
    assert os.path.isfile(src), "Source should be a file: "+src
    src = os.path.abspath(src)
    assert os.path.isabs(src), "Source path should be absolute: "+src
    assert os.path.exists(dest), "Destination does not exist: "+dest
    assert os.path.isdir(dest), "Destination should be a dir: "+dest
    dest = os.path.abspath(dest)
    assert os.path.isabs(dest), "Destination should be absolute: "+dest
    
    def handle_extracted_file(path):
        assert os.path.isabs(path), "Path should be absolute"
        if os.path.splitext(path)[1] in recursive_unzip_file_types:
            recursive_unzip(path, os.path.dirname(path), action)
            delete_file(path)                   # delete zip file, unzipped same location
        else:
            action(path)                        # basic file, perform action
            #delete_file(path)                  # no basic file clean up, leave for caller
        return
    
    extension = os.path.splitext(src)[1]        # dest file/dir will mirror old name
    dest = os.path.join(dest, os.path.basename(src.replace(extension,'')))
    assert extension in recursive_unzip_file_types, "Invalid extension: "+src
    assert os.path.isabs(dest), "New destination path not absolute: "+dest
    
    if extension == ".zip" or extension == ".tar" or extension == ".tgz": 
        logging.debug("Unzipping: %s", src)
        
        if not os.path.exists(dest):            # directory does not already exist...
            os.makedirs(dest)                   # make dir to unpack file contents
            
            error_flag = False
            try:                                # exception handling here only
                conans.tools.unzip(src, dest, keep_permissions=False)
            except Exception as e:
                logging.critical("Error during Conan unzip: %s", e)
                error_flag = True               # just log it and skip it
            
            if not error_flag:
                recursive_walk(dest, handle_extracted_file)# walk & unzip if need be
                #delete_directory(dest)         # no basic dir clean up, leave for caller
            elif os.path.exists(dest):
                delete_directory(dest)
        else:                                   # directory does exist...
            pass                                # skip the unzip step
        
    elif extension == ".gz":
        logging.debug("Decompressing: %s", src)
        
        if not os.path.exists(dest):            # file/dir does not already exist...
            error_flag = False
            try:                                # exception handling here only
                with gzip.open(src, "rb") as in_fd, open(dest, "wb") as out_fd:
                    while True:
                        data = in_fd.read(1000000)
                        if data == b'' or data == None or not data:
                            break
                        out_fd.write(data)
            except Exception as e:
                logging.critical("Error during GZip unzip: %s", e)
                error_flag = True               # just log it and skip it
            
            if not error_flag:
                if os.path.splitext(dest)[1] in recursive_unzip_file_types:
                    recursive_unzip(dest, os.path.dirname(dest), action)
                    delete_file(dest)           # delete zip file, unzipped same location
                else:
                    action(dest)                # basic file, perform action
                    #delete_file(dest)          # no basic file clean up, leave for caller
            elif os.path.exists(dest):
                if os.path.isdir(dest):
                    delete_directory(dest)
                else:
                    delete_file(dest)
        else:                                   # file/dir does exist...
            pass                                # skip the unzip step
    
    elif extension == ".7z":
        logging.debug("7z Decompressing: %s", src)
        
        if not os.path.exists(dest):            # directory does not already exist...
            os.makedirs(dest)                   # make dir to unpack file contents
            
            error_flag = False
            try:                                # exception handling here only
                pyunpack.Archive(src).extractall(dest)
            except Exception as e:
                logging.critical("Error during pyunpack extraction: %s", e)
                error_flag = True               # just log it and skip it
            
            if not error_flag:
                recursive_walk(dest, handle_extracted_file)# walk & unzip if need be
                #delete_directory(dest)         # no basic dir clean up, leave for caller
            elif os.path.exists(dest):
                delete_directory(dest)
        else:                                   # directory does exist...
            pass                                # skip the unzip step
    
    else:
        logging.critical("This execution path should never be reached")
        raise Exception("Seemingly impossible execution path")
    
    return


def recursive_walk(src, action):
    ''' Recursively walks deeply nested directories performing actions on each file.
    src : string
        path to source directory
    action : function(file_abspath) -> return None
        action function to take on each file
    '''
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


def delete_file(path):
    ''' Attempts to delete a file. If there is a problem halt the program.
    path : string
        absolute path of the file to delete
    '''
    assert os.path.isabs(path), "Path should be absolute: "+path
    
    try:
        os.remove(path)
    except Exception as exc:
        if isinstance(exc, OSError) and exc.errno == 13:
            if platform.system() != "Windows":
                logging.warning("Error deleting %s. Attempting to fix permissions", path)
                parent_dir = os.path.dirname(path)
                exit_code = os.system("chmod -R 755 {}".format(parent_dir))
                if exit_code != 0:
                  logging.warning("Bad exit code for chmod: %d %s", exit_code, path)
                try:
                    os.remove(path)             # try removing file again
                except Exception as exc:
                    logging.critical("Could not fix permissions: %d %s", exc.errno, exc)
                    raise exc                   # give up, tried everything
            else:
                logging.warning("Error deleting %s. Attempting to turn off read-only", path)
                os.chmod(path, stat.S_IWRITE)
                try:
                    os.remove(path)             # try removing file again
                except Exception as exc:
                    logging.critical("Could not fix permissions: %d %s", exc.errno, exc)
                    raise exc                   # give up, tried everything
        else:
            logging.critical("Problem deleting file: %s", exc)
            raise exc
    
    return


def delete_directory(path):
    ''' Attempts to delete a directory. If there is a problem halt the program.
    path : string
        absolute path of the directory to delete
    '''
    assert os.path.isabs(path), "Path should be absolute: "+path
    
    def handle_errors(func, path, excinfo):
        ''' Handles errors thrown by shutil.rmtree when trying to remove directories w/
        bad permissions. This elegant solution was originally found here:
        https://stackoverflow.com/questions/1889597/deleting-directory-in-python
        '''
        (t,exc,traceback) = excinfo
        if isinstance(exc, OSError) and exc.errno == 13:
            if platform.system() != "Windows":
                logging.warning("Error deleting %s. Attempting to fix permissions", path)
                parent_dir = os.path.dirname(path)
                exit_code = os.system("chmod -R 755 {}".format(parent_dir))
                if exit_code != 0:
                  logging.warning("Bad exit code for chmod: %d %s", exit_code, path)
                func(path)                      # try removing file again
            else:
                logging.warning("Error deleting %s. Attempting to turn off read-only", path)
                os.chmod(path, stat.S_IWRITE)   # turn off read-only
                func(path)                      # try removing file again
        else:
            logging.warning("Unknown exception occured during directory removal")
            logging.warning(excinfo)
            logging.warning(exc)
            raise exc
        return
    
    try:
        shutil.rmtree(path,onerror=handle_errors)
    except Exception as exc:
        logging.critical("Problem deleting unzipped folder: %s", exc)
        raise exc
    
    return
    
