kissdb
======

(Keep It) Simple Stupid Database

KISSDB is about the simplest key/value store you'll ever see, anywhere.
It's written in plain vanilla C using only the standard string and FILE
I/O functions, and should port to just about anything with a disk or
something that acts like one.

It stores keys and values of fixed length in a stupid-simple file format
based on fixed-size hash tables. If a hash collision occurrs, a new "page"
of hash table is appended to the database. The database is append-only;
there is no delete function. You can, however, change an existing value
without increasing the size of the database. (Put will replace existing
values.) The size of the hash table affects the trade-off between space
efficiency and performance. If you expect more keys, use larger tables.
But if you don't expect too many keys, large tables will waste space.

That being said, it's pretty flexible and lacks limitations. 64-bit values
are used, so there's no real file size limit. It's space-efficient, since
there's no meta-data other than the hash tables.

It implements no caching of its own except for the hash tables to speed
lookups, so you'll have to do that if you don't want to hit the disk for
every get(). It also implements no thread synchronization, so it should be
guarded by a mutex or something for multi-threaded apps. It presently
has no awareness of byte order. Most everyting these days is little-endian,
so that should be considered the "standard" for KISSDB database files.
So on big-endian systems byte swapping will need to be added in the
hash table I/O code.

Iteration over all values is supported, though values are unordered.

If you want something full-featured, check out SQLite or Berkeley DB. If
you want something super-duper-fast, check out Kyoto Cabinet. Both of these
are substantially larger though. If all you want to do is remember stuff
and look it up by key, you might consider this. Performance isn't bad for
something so trivial, especially if your OS has efficient I/O.

KISSDB is in the public domain, too. One reason it was written was the
poverty of simple key/value databases with wide open licensing. Even old
ones like GDBM have GPL, not LGPL, licenses.

See comments in kissdb.h for documentation. Makefile can be used to build
a test program on systems with gcc.

Author: Adam Ierymenko / ZeroTier Networks LLC
