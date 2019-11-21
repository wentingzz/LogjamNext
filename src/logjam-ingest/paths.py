"""
@author Nathaniel Brooks

Utility file system path functionality, such as the QuantumEntry class.
"""


import os


class QuantumEntry:
    """
    Represents a file system entry that could exist in multiple source directories
    at once. For the purposes of the Logjam system only the relative path after
    the source directory is relevant. The source directory is only used when
    locating a file absolutely.
    
    The design of this class loosely mimics the Python library class `pathlib.Path`.
    """
    
    def __init__(self, source, relative):
        """ Initializes an object with one source directory and a relative path """
        self.srcpath = source
        self.relpath = relative
    
    def __eq__(self, other):
        """ Returns whether two QuantumEntry objects are equal """
        if not isinstance(other, QuantumEntry):
            raise NotImplementedError("Can only compare QuantumEntry")
        
        return self.srcpath == other.srcpath and self.relpath == other.relpath
    
    def __truediv__(self, new_path):
        """ Returns a new QuantumEntry object where new_path is appended to the relative path """
        if not isinstance(new_path, str):
            raise NotImplementedError("Can only append str")
        
        return QuantumEntry(self.srcpath, os.path.join(self.relpath, new_path))
    
    def __itruediv__(self, new_path):
        """ Appends new_path to this QuantumEntry object's relative path """
        if not isinstance(new_path, str):
            raise NotImplementedError("Can only append str")
        
        self.relpath = os.path.join(self.relpath, new_path)
        return self
    
    @property
    def srcpath(self):
        """ Returns the source directory for this entry """
        return self._source
    
    @srcpath.setter
    def srcpath(self, source):
        """ Sets the source directory for this entry """
        self._source = source
        self._srcpath_trim_trailing_slash()
    
    def _srcpath_trim_trailing_slash(self):
        """ Trims the trailing slash from source path if it exists """
        while self._source.endswith("/") and self._source != "/":
            self._source = self._source[:-1]
    
    @property
    def relpath(self):
        """ Returns the relative location of the entry to its source directory """
        return self._relative
    
    @relpath.setter
    def relpath(self, relative):
        """ Sets the relative directory for this entry """
        assert not relative.startswith("/"), "Cannot start with '/' : "+relative
        
        self._relative = relative
        self._relpath_trim_trailing_slash()
    
    def _relpath_trim_trailing_slash(self):
        """ Trims the trailing slash from relative path if it exists """
        while self._relative.endswith('/'):
            self._relative = self._relative[:-1]
    
    @property
    def abspath(self):
        """ Returns the absolute location of the entry on the file system """
        return os.path.abspath(os.path.join(self.srcpath, self.relpath))
    
    @property
    def fullpath(self):
        """ Returns the full path of the entry, which is the source + relative path """
        return os.path.join(self.srcpath, self.relpath)
    
    @property
    def basename(self):
        """ Returns the base name of the entry as defined by `os.path.basename` """
        return os.path.basename(self.relpath)
    
    @property
    def extension(self):
        """
        Returns the extension of the entry (if the extension exists) including
        the dot before the extension name. Directories should not have an extension
        and if so this function will return an empty string. Leading dots are ignored.
        """
        return os.path.splitext(self.relpath)[1]
    
    def exists(self):
        """ Returns whether this entry exists on the file system """
        return os.path.exists(self.abspath)
    
    def is_dir(self):
        """ Returns whether this entry is a directory """
        return os.path.isdir(self.abspath)
    
    def is_file(self):
        """ Returns whether this entry is a file """
        return os.path.isfile(self.abspath)
