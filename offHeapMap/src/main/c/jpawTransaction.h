#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <jni.h>
#include <lz4.h>
#include "jpawMap.h"

#define SEPARATE_COMMITTED_VIEW
#define ADD_INDEX

#undef DEBUG
//#define DEBUG

// JNI reference: see http://www3.ntu.edu.sg/home/ehchua/programming/java/JavaNativeInterface.html


// map types
#define MAP_TYPE_DATA           0x00
#define MAP_TYPE_DATA_AND_VIEW  0x01
#define MAP_TYPE_INDEX          0x02
#define MAP_TYPE_INDEX_AND_VIEW 0x03

#define MAP_TYPE_VIEW_MASK      0x01        // map type & view mask != 0 => map or index has a committed view
#define MAP_TYPE_MASK           0x0f


// transaction modes. bitwise ORed, they go into the modes field of current_transaction

#define TRANSACTIONAL 0x01     // allow to rollback / safepoints
#define REDOLOG_ASYNC 0x02     // allow replaying on a different database - fast
#define REDOLOG_SYNC 0x04      // allow replaying on a different database - safe
#define AS_PER_TRANSACTION (-1)   // no override in map


#define IS_TRANSACTIONAL(ctx, mapdata)  ((ctx) && (mapdata)->modes != 0 && (ctx)->modes != 0)
#define TX_LOG_ENTRIES_PER_CHUNK_LV1    1024        // first level blocks
#define TX_LOG_ENTRIES_PER_CHUNK_LV2     256        // changes in final block


// stores a single modification of the maps.
// the relevant types are insert / update / remove.
// The type can be determined by which ptr is null and which not.
// the "nextSameHash" entries are "misused"
struct tx_log_entry {
    struct map *affected_table;
    struct dataEntry *old_entry;
    struct dataEntry *new_entry;
};


struct tx_log_list {
    struct tx_log_entry entries[TX_LOG_ENTRIES_PER_CHUNK_LV2];
};

// global variables
struct tx_log_hdr {
    int number_of_changes;          // (uncommitted) row changes pending in the current transaction
    int modes;                      // if tx mode is enabled
    jlong changeNumberDelta;
    jlong currentTransactionRef;
    jlong lastCommittedRef;
    jlong lastCommittedRefOnViews;
    struct tx_log_list *chunks[TX_LOG_ENTRIES_PER_CHUNK_LV1];
};

