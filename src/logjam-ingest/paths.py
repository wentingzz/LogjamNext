"""
@author Nathaniel Brooks


"""


import os



class QuantumDirectories:
	"""
	Represents a collection of directories that a single file or directory
	might simultaneously exist in. This will be used to detect overlapping
	files between two distinct directories with the same hierarchy.
	"""


class Entry:
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
        """ Returns whether two Entry objects are equal """
        if not isinstance(other, Entry):
            raise NotImplementedError("Can only compare Entry")
        
        return self._source == other._source and self._relative == other._relative
    
    def __truediv__(self, new_path):
        """ Returns a new Entry object where new_path is appended to the relative path """
        return Entry(self._source, os.path.join(self._relative, new_path))
    
    def __itruediv__(self, new_path):
        """ Appends new_path to this Entry object's relative path """
        self._relative = os.path.join(self._relative, new_path)
    
    @property
    def abspath(self):
        """ Returns the absolute location of the entry on the file system """
        return os.path.abspath(os.path.join(self._source, self._relative))
    
    @property
    def fullpath(self):
        """ Returns the full path of the entry, which is the source + relative path """
        return os.path.join(self._source, self._relative)
    
    @property
    def srcpath(self):
        """ Returns the source directory for this entry """
        return self._source
    
    @srcpath.setter
    def srcpath(self, source):
        """ Sets the source directory for this entry """
        source = source[:-1] if source.endswith("/") and source != "/" else source
        self._source = source                       # removed trailing /, format = ...n/n
    
    @property
    def relpath(self):
        """ Returns the relative location of the entry to its source directory """
        return self._relative
    
    @relpath.setter
    def relpath(self, relative):
        """ Sets the relative directory for this entry """
        assert not relative.startswith("/"), "Cannot start with '/' : "+relative
        assert not relative.startswith("./"), "Leading . not supported : "+relative
        assert not relative == ".", "Leading . not supported : "+relative
        assert not relative.startswith("../"), "Leading .. not supported : "+relative
        assert not relative == "..", "Leading .. not supported : "+relative
        
        relative = relative[:-1] if relative.endswith('/') else relative
        self._relative = relative                   # removed trailin /, format = n/n...
    
    @property
    def basename(self):
        """ Returns the base name of the entry as defined by `os.path.basename` """
        return os.path.basename(self._relative)
    
    @property
    def extension(self):
        """
        Returns the extension of the entry (if the extension exists) including
        the dot before the extension name. Directories should not have an extension
        and if so this function will return an empty string. Leading dots are ignored.
        """
        return os.path.splitext(self._relative)[1]
    
    def exists(self):
        """ Returns whether this entry exists on the file system """
        return os.path.exists(self.abspath)
    
    def is_dir(self):
        """ Returns whether this entry is a directory """
        return os.path.isdir(self.abspath)
    
    def is_file(self):
        """ Returns whether this entry is a file """
        return os.path.isfile(self.abspath)
    
    

