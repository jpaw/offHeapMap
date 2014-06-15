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

Being a simple key / value store, the implementation is agnostic of the contents. A map could correspond
 - to all tables of a database (the values being the rows in serialized form, including the information which table they belong to)
 - to a regular database table (assumingly the default approach)
 - to a partition of a table (for example all data of a table belonging to a specific tenant)


## Limitations

The build files run on Linux / gcc only. No effort has been spent to make the build portable. As mentioned above, it serves mainly as a feasibility study.

All classes are non-threadsafe. Following ideas of the LMAX disruptor (http://lmax-exchange.github.io/disruptor/), you can operate
single-threaded as long as you're fast enough. You can however create multiple independent transactions.
In case your map corresponds to a partition of a classical database table (for example data of a specific tenant), you can run a separate thread
per tenant in parallel. 

Due to the very simple internal data structures, transactions are currently limited to 256,000 row changes.


## Compatibility notes

The build uses Java 8 (Oracle JDK), as it's the current environment, but there are not Java 8 specific features used. You could easily backport it to Java 6 or 7. 


## Revision numbering

All commits using SNAPSHOT revisions are considered experimental and potentially incompete, in fact may not even compile.
Commits to fixed revisions are considered stable, they should compile and pass all tests.
After a first such revision, the master branch will always contain the latest of these releases, while the develop branch will contain the latest SNAPSHOT revision (HEAD).
Before any 1.0.0 release, all APIs are considered to be in flux. (No promise there will ever be a 1.0.0 release.)


## Roadmap

Support for the following features is planned for subsequent releases:
 - writing redo logs synchronously or asynchronously for replication to shadow databases (using chronicle: https://github.com/peter-lawrey/Java-Chronicle)
 - persisting map storage to disk
 - master / master replication / conflict detection
 - internal read-only shadow (replaying committed changes only) for parallel execution of queries#
 - secondary unique and non-unique indexes (hash or Btree) for simple queries
