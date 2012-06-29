wolfdedup
=========

A poor man's offline file-level deduplication solution.

wolfdedup searches your files for exact duplicates (same size, md5 and 
sha256), then creates an hardlink to save space on disk.

As you know, Mr. Wolf solves problems, and this script is intended for
messy fileservers that you know have a lot of identical files spread
over multiple directories.

Please, if you don't know what an hardlink is and how it works, avoid
using this script, it's safer.


