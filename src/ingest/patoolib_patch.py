"""
Performs a swap of a single function in patoolib to allow
extracting password protected 7zip archives non-interactively
"""

def patch_7z(patoolib):
    """
    Carries out patch of the function 'get_archive_cmdlist_func'
    on the given imported instance of patoolib
    """
    orig_cmdlist_fn = patoolib.get_archive_cmdlist_func

    def extract_7z(archive, compression, cmd, verbosity, interactive, outdir):
        """Extract a 7z archive. Patched to provide a phony password. """
        cmdlist = [cmd, 'x']
        if not interactive:
            cmdlist.append('-y')
        cmdlist.extend(['-o%s' % outdir, '-pfoo', '--', archive])
        return cmdlist

    def new_get_archive_cmdlist_func(program, command, format):
        """
        Dynamically finds the function to use for unzipping.
        Forces our patched function for extracting 7zips
        """
        if command == 'extract' and format == '7z':
            return extract_7z
        else:
            return orig_cmdlist_fn(program, command, format)

    # Patch override function into imported instance of patoolib
    patoolib.get_archive_cmdlist_func = new_get_archive_cmdlist_func

