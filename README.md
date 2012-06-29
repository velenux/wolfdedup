WolfDedup
=========

A poor man's offline file-level deduplication solution. In Ruby.

wolfdedup searches your files for exact duplicates (same size, md5 and 
sha256), then creates an hardlink to save space on disk.

As you know, Mr. Wolf solves problems, and this script is intended for
messy fileservers that you know have a lot of identical files spread
over multiple directories.

Please, if you don't know what an hardlink is and how it works, avoid
using this script, it's safer.

Requirements
------------

 * Ruby (tested on 1.9.2 and 1.9.3)
 * DataMapper gems (gem install data_mapper)
 * A DataMapper adapter (only tested with dm-mysql-adapter)


