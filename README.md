# offHeapMap - A JNI performance study

This repository contains a performance study, what's achievable using JNI.
It offers an implementation of a key / value store (Map<long,byte []>) outside of the Java heap,
therefore not affected by garbage collection (GC).

It aims to provide a usable implementation, and therefore integrates the LZ4 compression 
library https://github.com/Cyan4973/lz4 to provide a compact storage.

The implementation also provides basic transactional functionality:
 - combining changes on multiple maps into a transaction
   - commit
   - rollback
   - setting a safepoint, rollback to a safepoint
 - autonomous operations on selected maps
 - (with 0.0.2): optional second view, one for the current (dirty uncommitted), one which is updated only after a successful commit
 - (with 0.0.2) persisting map storage to disk - preliminary - no error checking is done yet

Being a simple key / value store, the implementation is agnostic of the contents. A map could correspond
 - to all tables of a database (the values being the rows in serialized form, including the information which table they belong to)
 - to a regular database table (assumingly the default approach)
 - to a partition of a table (for example all data of a table belonging to a specific tenant)


## Limitations

The build files run on Linux / gcc 64 bit only, using Oracle's 64 bit JDK8. No effort has been spent to make the build portable. As mentioned above, it serves mainly as a feasibility study.

All native libraries are assumed to reside within the lib subdirectory of the user's home directory.

The build files assume Eclipse 4.4 LUNA with CDT.

All classes are non-threadsafe. Following ideas of the LMAX disruptor (http://lmax-exchange.github.io/disruptor/), you can operate
single-threaded as long as you're fast enough. You can however create multiple independent transactions.
In case your map corresponds to a partition of a classical database table (for example data of a specific tenant), you can run a separate thread
per tenant in parallel. 

Due to the very simple internal data structures, transactions are currently limited to 256,000 row changes.


## Compatibility notes

The build uses Java 8 (Oracle JDK), as it's the current environment.


## Revision numbering

All commits using SNAPSHOT revisions are considered experimental and potentially incompete, in fact may not even compile.
Commits to fixed revisions are considered stable, they should compile and pass all tests.
After a first such revision, the master branch will always contain the latest of these releases, while the develop branch will contain the latest SNAPSHOT revision (HEAD).
Before any 1.0.0 release, all APIs are considered to be in flux. (No promise there will ever be a 1.0.0 release.)


## Roadmap

Support for the following features is planned for subsequent releases:
 - writing redo logs synchronously or asynchronously for replication to shadow databases (using chronicle: https://github.com/peter-lawrey/Java-Chronicle)
 - master / master replication / conflict detection
 - secondary unique and non-unique indexes (hash or Btree) for simple queries. Iterator for non-unique keys
 - compacting redo logs (discarding subsequent changes on the same key within a single transaction)
 - add commit ref to table structure (required to identify conflicts in master/master replication mode, also allows optimistic locking)
 - operations deleteIf(key, oldref) / setIf(key, value, oldref)
 - optimize redo / rollback: compare entry ptrs instead of keys
