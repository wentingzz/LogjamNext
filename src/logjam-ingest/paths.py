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
    """
    
    def __init__(self, source, relative):
        """ Initializes an object with one source directory and a relative path """
        assert not relative.startswith("/"), "Cannot start with '/' : "+relative
        assert not relative.startswith("./"), "Leading . not supported : "+relative
        assert not relative == ".", "Leading . not supported : "+relative
        assert not relative.startswith("../"), "Leading .. not supported : "+relative
        assert not relative == "..", "Leading .. not supported : "+relative
        
        source = source[:-1] if source.endswith("/") and source != "/" else source
        self._source = source                       # removed trailing /, format = ...n/n
        
        relative = relative[:-1] if relative.endswith('/') else relative
        self._relative = relative                   # removed trailin /, format = n/n...
    
    @property
    def abspath(self):
        """ Returns the absolute location of the entry on the file system """
        return os.path.abspath(os.path.join(self._source, self._relative))
    
    @property
    def fullpath(self):
        """ Returns the full path of the entry, which is the source + relative path"""
        return os.path.join(self._source, self._relative)
    
    @property
    def srcpath(self):
        """ Returns the source directory for this entry """
        return self._source
    
    @property
    def relpath(self):
        """ Returns the relative location of the entry to its source directory """
        return self._relative
    
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
    
    def isdir(self):
        """ Returns whether this entry is a directory """
        return os.path.isdir(self.abspath)
    
    def isfile(self):
        """ Returns whether this entry is a file """
        return os.path.isfile(self.abspath)
    
    

