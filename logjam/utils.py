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
        
        assert not os.path.exists(dest), "Directory should not already exist: "+dest
        os.makedirs(dest)                       # make dir to unpack file contents
        
        try:                                    # exception handling here only
            conans.tools.unzip(src, dest, keep_permissions=False)
        except Exception as e:
            logging.critical("Error during Conan unzip: %s", e)
            raise e                             # expand as exceptions are discovered
        
        recursive_walk(dest, handle_extracted_file)# walk & unzip if need be
        #delete_directory(dest)                 # no basic dir clean up, leave for caller
        
    elif extension == ".gz":
        logging.debug("Decompressing: %s", src)
        
        try:                                    # exception handling here only
            decompressed_file_data = gzip.GzipFile(src, "rb").read()
            open(dest, "wb").write(decompressed_file_data)
        except Exception as e:
            logging.critical("Error during GZip unzip: %s", e)
            raise e                             # expand as exceptions are discovered
        
        if os.path.splitext(dest)[1] in recursive_unzip_file_types:
            recursive_unzip(dest, os.path.dirname(dest), action)
            delete_file(dest)                   # delete zip file, unzipped same location
        else:
            action(dest)                        # basic file, perform action
            #delete_file(dest)                  # no basic file clean up, leave for caller
    
    elif extension == ".7z":
        logging.debug("7z Decompressing: %s", src)
        
        assert not os.path.exists(dest), "Directory should not already exist: "+dest
        os.makedirs(dest)                       # make dir to unpack file contents
        
        try:                                    # exception handling here only
            pyunpack.Archive(src).extractall(dest)
        except Exception as e:
            logging.critical("Error during pyunpack extraction: %s", e)
            raise e                             # expand as exceptions are discovered
        
        recursive_walk(dest, handle_extracted_file)# walk & unzip if need be
        #delete_directory(dest)                 # no basic dir clean up, leave for caller
    
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
    assert os.path.exists(path), "Path does not exist: "+path
    assert not os.path.isdir(path), "Path is not a file: "+path
    assert os.path.isabs(path), "Path should be absolute: "+path
    
    try:
        os.remove(path)
    except Exception as exc:
        if isinstance(exc, OSError) and exc.errno == 13:
            if platform.system() != "Windows":
                logging.warning("Error deleting %s. Attempting to fix permissions", path)
                os.system("chmod -R 755 {}".format(scratchDirRoot))
                try:
                    os.remove(path)             # try removing file again
                except Exception as exc:
                    logging.critical("Problem deleting file: %d %s", e.errno, e)
                    raise exc                   # give up, tried everything
            else:
                logging.warning("Error deleting %s. Attempting to turn off read-only", path)
                os.chmod(path, stat.S_IWRITE)
                try:
                    os.remove(path)             # try removing file again
                except Exception as exc:
                    logging.critical("Problem deleting file: %d %s", e.errno, e)
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
    assert os.path.exists(path), "Path does not exist: "+path
    assert os.path.isdir(path), "Path is not a directory: "+path
    
    def handle_errors(func, path, excinfo):
        ''' Handles errors thrown by shutil.rmtree when trying to remove directories w/
        bad permissions. This elegant solution was originally found here:
        https://stackoverflow.com/questions/1889597/deleting-directory-in-python
        '''
        (t,exc,traceback) = excinfo
        if isinstance(exc, OSError) and exc.errno == 13:
            if platform.system() != "Windows":
                logging.warning("Error deleting %s. Attempting to fix permissions", path)
                os.system("chmod -R 755 {}".format(scratchDirRoot))
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
    
