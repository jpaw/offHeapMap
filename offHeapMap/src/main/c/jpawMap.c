#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <jni.h>
#include <lz4.h>
#include "jpawMap.h"

#define USE_CRITICAL_FOR_STORE
// #define USE_CRITICAL_FOR_RETRIEVAL       // this seems to be much slower!

#define SEPARATE_COMMITTED_VIEW

#undef DEBUG
//#define DEBUG



// round the size to be allocated to the next multiple of 16, because malloc anyway returns multiples of 16.
#define ROUND_UP_SIZE(size)     ((((size) - 1) & ~0x0f) + 16)
#define ROUND_UP_FILESIZE(size) ((((size) - 1) & ~0x07) + 8)  // as stored in the disk dump file

struct entry {
    // start with the internal ptr, to allow writing a continuous area when dumping the file.
    struct entry *nextSameHash;
#ifdef SEPARATE_COMMITTED_VIEW
    struct entry *nextInCommittedView;
#endif
    // from here, the entry is dumped to disk on saves.
    int uncompressedSize;       // size of the data, without this header portion
    int compressedSize;         // 0 = is not compressed, otherwise size of the compressed output. The actual allocated space is rounded up such that the entry size is always a multiple of 16
    jlong key;
    char data[];
};

struct map {
    struct entry **data;
    int hashTableSize;
    int modes;              // 00 = not transactional, -1 = take mode of transaction, 1 = TRANSACTIONAL
    int count;              // current number of entries. tracking this in Java gives a faster size() operation, but causes problems (callback required) for rollback
    int padding;
    jlong lastCommittedRef;
    struct map *committedView;  // same data, but synched after commit (to provide secondary view for read/only queries)
};

// stores a single modification of the maps.
// the relevant types are insert / update / remove.
// The type can be determined by which ptr is null and which not.
// the "nextSameHash" entries are "misused"
struct tx_log_entry {
    struct map *affected_table;
    struct entry *old_entry;
    struct entry *new_entry;
};


struct tx_delayed_update {
    jlong lastCommittedRef;         // this must be the predecessor
    jlong currentTransactionRef;
    int numberOfChanges;            // how many rows have been affected?
    int padding;
    struct tx_log_entry transactions [];
};


#define TX_LOG_ENTRIES_PER_CHUNK_LV1    1024        // first level blocks
#define TX_LOG_ENTRIES_PER_CHUNK_LV2     256        // changes in final block


// transaction modes. bitwise ORed, they go into the modes field of current_transaction

#define TRANSACTIONAL 0x01     // allow to rollback / safepoints
#define REDOLOG_ASYNC 0x02     // allow replaying on a different database - fast
#define REDOLOG_SYNC 0x04      // allow replaying on a different database - safe
#define AS_PER_TRANSACTION (-1)   // no override in map


#define IS_TRANSACTIONAL(ctx, mapdata)  ((ctx) && (mapdata)->modes != 0 && (ctx)->modes != 0)

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

static jfieldID javaIteratorCurrentHashIndexFID;
static jfieldID javaIteratorCurrentKeyFID;


// reference: see http://www3.ntu.edu.sg/home/ehchua/programming/java/JavaNativeInterface.html




void throwOutOfMemory(JNIEnv *env) {
    jclass exceptionCls = (*env)->FindClass(env, "java/lang/RuntimeException");
    (*env)->ThrowNew(env, exceptionCls, "Out of off-heap memory in JNI call");
}
void throwAny(JNIEnv *env, char *msg) {
    jclass exceptionCls = (*env)->FindClass(env, "java/lang/RuntimeException");
    (*env)->ThrowNew(env, exceptionCls, msg);
}

// forwards...
jboolean execRemove(struct tx_log_hdr *ctx, struct map *mapdata, jlong key);
jboolean execRemoveShadow(struct map *mapdata, jlong key);
struct entry * setPutSub(struct map *mapdata, struct entry *newEntry);
struct entry * setPutSubShadow(struct map *mapdata, struct entry *newEntry);


// Iterator
/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natInit
 * Signature: (Ljava/lang/Class;)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_natInit
  (JNIEnv *env, jclass myClass, jclass iteratorClass) {

    // Get the Field ID of the instance variables "number"
    javaIteratorCurrentKeyFID = (*env)->GetFieldID(env, iteratorClass, "currentKey", "J");
    if (!javaIteratorCurrentKeyFID) {
        throwAny(env, "Invoking class must have a field long currentKey");
        return;
    }
    javaIteratorCurrentHashIndexFID = (*env)->GetFieldID(env, iteratorClass, "currentHashIndex", "I");
    if (!javaIteratorCurrentHashIndexFID) {
        throwAny(env, "Invoking class must have a field int currentHashIndex");
        return;
    }
#ifdef DEBUG
    fprintf(stderr, "Iterator INIT complete. key FID is %d, hashIndex FID is %d\n", (int)javaIteratorCurrentKeyFID, (int)javaIteratorCurrentHashIndexFID);
#endif
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_PrimitiveLongKeyOffHeapMapEntryIterator
 * Method:    natIterate
 * Signature: (JJI)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_00024PrimitiveLongKeyOffHeapMapEntryIterator_natIterate
  (JNIEnv *env, jobject myClass, jlong cMap, jlong nextEntryPtr, jint hashIndex) {
    struct map *mapdata = (struct map *)cMap;
    struct entry *e = (struct entry *)nextEntryPtr;
#ifdef DEBUG
    fprintf(stderr, "iterate on map %16p (has %d entries in %d slots)\n", mapdata, mapdata->count, mapdata->hashTableSize);
#endif
    if (e) {
        e = e->nextSameHash;
        if (e) {
            // update Key, but not slot
#ifdef DEBUG
            fprintf(stderr, "Found an entry with key %ld in same slot %d\n", (long)(e->key), hashIndex);
#endif
            (*env)->SetLongField(env, myClass, javaIteratorCurrentKeyFID, e->key);
            return (jlong)e;
        }
    }

    // need a new slot for the next entry
    // must search for an entry in the next slot...
    while (++hashIndex < mapdata->hashTableSize) {
        e = mapdata->data[hashIndex];
        if (e) {
            // found an entry. Store this new hashIndex and return the entry
#ifdef DEBUG
            fprintf(stderr, "Found an entry with key %ld in new slot %d\n", (long)(e->key), hashIndex);
#endif
            (*env)->SetIntField(env, myClass, javaIteratorCurrentHashIndexFID, hashIndex);
            (*env)->SetLongField(env, myClass, javaIteratorCurrentKeyFID, e->key);
            return (jlong)e;
        }
    }
    // no further entry found. must be at end
    return (jlong)0;
}

// transactions

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCreateTransaction
 * Signature: (IJ)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCreateTransaction
  (JNIEnv *env, jobject tx, jint mode, jlong changeNumberDelta) {
    struct tx_log_hdr *hdr = malloc(sizeof(struct tx_log_hdr));
    if (!hdr) {
        throwOutOfMemory(env);
        return (jlong)0;
    }
    memset(hdr, 0, sizeof(struct tx_log_hdr));
    hdr->modes = mode;
    hdr->number_of_changes = 0;
    hdr->lastCommittedRef = (jlong)-1L;
    hdr->lastCommittedRefOnViews = (jlong)-1L;
    hdr->changeNumberDelta = changeNumberDelta;
    hdr->currentTransactionRef = changeNumberDelta;
#ifdef DEBUG
    fprintf(stderr, "CREATE TRANSACTION\n");
#endif
    return (jlong)hdr;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCloseTransaction
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCloseTransaction
  (JNIEnv *env, jobject me, jlong cTx) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    if (hdr->number_of_changes) {
        throwAny(env, "Cannot close within pending transaction");
        return;
    }
    // free redo blocks
    int i;
    for (i = 0; i < TX_LOG_ENTRIES_PER_CHUNK_LV1 && hdr->chunks[i]; ++i) {
        free(hdr->chunks[i]);
    }
    free(hdr);
#ifdef DEBUG
    fprintf(stderr, "CLOSE TRANSACTION\n");
#endif
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natSetMode
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natSetMode
  (JNIEnv *env, jobject me, jlong cTx, jint mode) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    if (hdr->number_of_changes) {
        throwAny(env, "Cannot change mode within pending transaction");
        return;
    }
    hdr->modes = mode;
}

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natBeginTransaction
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natBeginTransaction
(JNIEnv *env, jobject me, jlong cTx, jlong newRef) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    if (hdr->number_of_changes) {
        throwAny(env, "Cannot begin transaction while changes are pending");
        return;
    }
    hdr->currentTransactionRef = newRef;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natSetSafepoint
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natSetSafepoint
  (JNIEnv *env, jobject me, jlong cTx) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    return hdr->number_of_changes;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCommitDelayedUpdate
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCommitDelayedUpdate
    (JNIEnv *env, jobject me, jlong cTx) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    int number_of_changes = hdr->number_of_changes;
    if (!number_of_changes) {
        return (jlong)0;
    }
    struct tx_delayed_update *upd = malloc(sizeof(struct tx_delayed_update) + sizeof(struct tx_log_entry) * number_of_changes);
    if (!upd) {
        throwOutOfMemory(env);
        return (jlong)0;
    }
    upd->lastCommittedRef = hdr->lastCommittedRef;
    upd->currentTransactionRef = hdr->currentTransactionRef;
    upd->numberOfChanges = number_of_changes;
    upd->padding = 0;

    int i = 0;
    int j = 0;  // slot number
    while (i + TX_LOG_ENTRIES_PER_CHUNK_LV2 < number_of_changes) {  // a loop for all but the last one
        memcpy(&(upd->transactions[i]), hdr->chunks[j], sizeof(struct tx_log_entry) * TX_LOG_ENTRIES_PER_CHUNK_LV2);
        ++j;
        i += TX_LOG_ENTRIES_PER_CHUNK_LV2;
    }
    memcpy(&(upd->transactions[i]), hdr->chunks[j], sizeof(struct tx_log_entry) * (number_of_changes - i));
    hdr->number_of_changes = 0;
    hdr->lastCommittedRef = hdr->currentTransactionRef;
    hdr->currentTransactionRef += hdr->changeNumberDelta;
    return (jlong)upd;
}

void replayUpdateOnView(struct tx_log_hdr *hdr, struct tx_log_entry *ep) {
    ep->affected_table->lastCommittedRef = hdr->currentTransactionRef;
    struct map *view = ep->affected_table->committedView;
    if (!view) {
        // no shadow: simple rule: discard old entry.
        struct entry *e = ep->old_entry;
        if (e)
            free(e);
    } else {
        // have secondary view. Do not discard old entry, because we either still need it, or we discard it within a recursive call
        // we have a view, and are asked to replay the tx on it
        if (!ep->new_entry) {
            // was a remove => remove it on the view (which frees the old data)
            execRemoveShadow(view, ep->old_entry->key);
        } else {
            // insert or replace
            struct entry *shouldBeOld = setPutSubShadow(view, ep->new_entry);
            if (shouldBeOld != ep->old_entry)
                fprintf(stderr, "REDO PROBLEM: expected to get %16p, but got %16p for key %ld\n", ep->old_entry, shouldBeOld, ep->new_entry->key);
            // if new_entry was not null, then free it (it is no longer required)
            if (ep->old_entry)
                free(ep->old_entry);
        }
        view->lastCommittedRef = hdr->currentTransactionRef;
    }
}

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natUpdateViews
 * Signature: (JJ)V
 */
JNIEXPORT int JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natUpdateViews
(JNIEnv *env, jobject me, jlong cTx, jlong transactions) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    struct tx_delayed_update *upd = (struct tx_delayed_update *)transactions;
    if (upd->lastCommittedRef != hdr->lastCommittedRefOnViews) {
        throwAny(env, "Invalid sequence of replays");
        return 0;
    }
    int numberOfChanges = upd->numberOfChanges;
    int i;
    struct tx_log_entry *ep = upd->transactions;
    for (i = 0; i < numberOfChanges; ++i) {
        replayUpdateOnView(hdr, ep++);
    }

    hdr->lastCommittedRefOnViews = upd->currentTransactionRef;
    free(upd);
    return numberOfChanges;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCommit
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCommit
  (JNIEnv *env, jobject me, jlong cTx) {
    // currently no redo logs are written. Therefore just discard the entries
#ifdef DEBUG
    fprintf(stderr, "COMMIT START\n");
#endif
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    int currentEntries = hdr->number_of_changes;
    if (currentEntries) {
        struct tx_log_list *chunk = NULL;
        int i;
        for (i = 0; i < currentEntries; ++i) {
            if (!(i & 0xff)) {
                // need a new chunk
                chunk = hdr->chunks[i >> 8];
            }
            replayUpdateOnView(hdr, &(chunk->entries[i & 0xff]));
        }
    }
    hdr->number_of_changes = 0;
    hdr->lastCommittedRef = hdr->currentTransactionRef;
    hdr->currentTransactionRef += hdr->changeNumberDelta;
#ifdef DEBUG
    fprintf(stderr, "COMMIT END\n");
#endif
    return currentEntries;
}

void rollback(struct tx_log_entry *e) {
#ifdef DEBUG
    fprintf(stderr, "Rolling back entry for %16p %16p %16p\n", e->affected_table, e->old_entry, e->new_entry);
#endif
    if (!e->old_entry) {
        // was an insert
#ifdef DEBUG
        fprintf(stderr, "    => remove key %ld\n", e->new_entry->key);
#endif
        execRemove(NULL, e->affected_table, e->new_entry->key); // TODO: assert new entry was returned
        // as old_entry was null, new_entry is definitely not null and must be deallocated
        // free(e->new_entry);  // but this was done within execRenove already!
    } else {
        // replace or delete
#ifdef DEBUG
        fprintf(stderr, "    => insert / update %16p with key %ld\n", e->old_entry, e->old_entry->key);
#endif
        struct entry *shouldBeNew = setPutSub(e->affected_table, e->old_entry);
        if (shouldBeNew != e->new_entry)
            fprintf(stderr, "ROLLBACK PROBLEM: expected to get %16p, but got %16p for key %ld\n", e->new_entry, shouldBeNew, e->old_entry->key);
        // if new_entry was not null, then free it (it is no longer required)
        if (e->new_entry)
            free(e->new_entry);
    }
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natDebugRedoLog
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natDebugRedoLog
  (JNIEnv *env, jobject me, jlong cTx) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    int currentEntries = hdr->number_of_changes;
    int i;
    for (i = 0; i < currentEntries; ++i) {
        struct tx_log_list *chunk = hdr->chunks[i >> 8];
        struct tx_log_entry *loge = &chunk->entries[i & 0xff];
        jlong key = loge->new_entry ? loge->new_entry->key : loge->old_entry->key;
        fprintf(stderr, "redo log entry %5d is for key %16ld: old=%16p, new=%16p\n", i, key, loge->old_entry, loge->new_entry);
    }
}



/** Record a row change. Returns an error if anything went wrong. */
char *record_change(struct tx_log_hdr *ctx, struct map *mapdata, struct entry *oldData, struct entry *newData) {
    if (!IS_TRANSACTIONAL(ctx, mapdata)) {
        // no transaction log. Maybe free old data
        if (oldData)
            free(oldData);
        return NULL;
    }
    int chunkIndex = ctx->number_of_changes >> 8;
    if (chunkIndex >= TX_LOG_ENTRIES_PER_CHUNK_LV1) {
        return "Too many row changes within transaction";
    }
    struct tx_log_list *chunk = ctx->chunks[chunkIndex];
    if (!chunk) {
        // must allocate
        chunk = malloc(sizeof(struct tx_log_list));
        if (!chunk)
            return "Out of memory recording rollback info";
        ctx->chunks[chunkIndex] = chunk;
    }
    struct tx_log_entry *loge = &(chunk->entries[ctx->number_of_changes & 0xff]);
#ifdef DEBUG
    fprintf(stderr, "Storing rollback data %6d at %16p: map=%16p old=%16p new=%16p\n", ctx->number_of_changes, loge, mapdata, oldData, newData);
#endif
    loge->affected_table = mapdata;
    loge->old_entry = oldData;
    loge->new_entry = newData;
    ++(ctx->number_of_changes);
    return NULL;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natRollback
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natRollback
  (JNIEnv *env, jobject me, jlong cTx, jint rollbackTo) {
#ifdef DEBUG
    fprintf(stderr, "ROLLBACK START (%d)\n", rollbackTo);
#endif
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    int currentEntries = hdr->number_of_changes;
    if (currentEntries > rollbackTo) {
        struct tx_log_list *chunk = hdr->chunks[currentEntries >> 8];
        while (currentEntries > rollbackTo) {
            if (!(currentEntries & 0xff))
                chunk = hdr->chunks[(currentEntries-1) >> 8];
            --currentEntries;
            rollback(&(chunk->entries[currentEntries & 0xff]));
        }
        hdr->number_of_changes = rollbackTo;
    }
#ifdef DEBUG
    fprintf(stderr, "ROLLBACK END\n");
#endif
}


/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natOpen
 * Signature: (IIZ)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natOpen
    (JNIEnv *env, jobject me, jint size, jint mode, jboolean withCommittedView) {
    // round up the size to multiples of 32, for the collision indicator
    size = ((size - 1) | 31) + 1;
    struct map *mapdata = malloc(sizeof(struct map));
    if (!mapdata) {
        throwOutOfMemory(env);
        return 0L;
    }
    mapdata->hashTableSize = size;
    mapdata->modes = mode;
    mapdata->count = 0;
    mapdata->padding = 0;
    mapdata->lastCommittedRef = -1L;
    mapdata->committedView = NULL;
    mapdata->data = calloc(size, sizeof(struct entry *));
    if (!mapdata->data) {
        free(mapdata);
        throwOutOfMemory(env);
        return 0L;
    }
    if (withCommittedView) {
        struct map *view = malloc(sizeof(struct map));
        if (!view) {
            free(mapdata->data);
            free(mapdata);
            throwOutOfMemory(env);
            return 0L;
        }
        memcpy(view, mapdata, sizeof(struct map));
        mapdata->committedView = view;

        view->modes = 0;        // the committed view does not have any TX management
        view->data = calloc(size, sizeof(struct entry *));
        if (!view->data) {
            free(view);
            free(mapdata->data);
            free(mapdata);
            throwOutOfMemory(env);
            return 0L;
        }
    }

    // printf("jpawMap: created new map at %p\n", mapdata);
    return (jlong) mapdata;
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natGetView
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natGetView
  (JNIEnv *env, jobject me, jlong cMap) {
    struct map *mapdata = (struct map *) cMap;
    return (jlong)mapdata->committedView;
}

int computeHash(jlong arg, int size) {
    arg *= 33;
    return ((int) (arg ^ (arg >> 32)) & 0x7fffffff) % size;
}

// clear all entries
void clear(struct entry **data, int numEntries) {
    int i;
    for (i = 0; i < numEntries; ++i) {
        struct entry *p = data[i];
        while (p) {
            register struct entry *next = p->nextSameHash;
            free(p);
            p = next;
        }
    }
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natClose
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natClose
    (JNIEnv *env, jobject me, jlong cMap) {
    // Get the int given the Field ID
    struct map *mapdata = (struct map *) cMap;
    clear(mapdata->data, mapdata->hashTableSize);
    free(mapdata->data);
    free(mapdata);
}


/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natGetSize
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_natGetSize
    (JNIEnv *env, jobject me, jlong cMap) {
    return ((struct map *)cMap)->count;
}


/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natClear
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natClear
    (JNIEnv *env, jobject me, jlong cMap, jlong ctxAsLong) {
    // Get the int given the Field ID
    struct map *mapdata = (struct map *) cMap;
    struct tx_log_hdr *ctx = (struct tx_log_hdr *)ctxAsLong;
    if (!IS_TRANSACTIONAL(ctx, mapdata)) {
        // this is faster
        clear(mapdata->data, mapdata->hashTableSize);
    } else {
        int hash;
        for (hash = 0; hash < mapdata->hashTableSize; ++hash) {
            struct entry *e;
            for (e = mapdata->data[hash]; e; e = e->nextSameHash) {
#ifdef DEBUG
                fprintf(stderr, "Transactional clear of entry %16p in hash slot %d\n", e, hash);
#endif
                record_change(ctx, mapdata, e, NULL);
            }
        }
    }
    memset(mapdata->data, 0, mapdata->hashTableSize * sizeof(struct entry *));     // set the initial pointers to NULL
    mapdata->count = 0;
}

static jbyteArray toJavaByteArray(JNIEnv *env, struct entry *e) {
    if (!e)
        return (jbyteArray) 0;
    jbyteArray result = (*env)->NewByteArray(env, e->uncompressedSize);
    if (!e->compressedSize) {
        (*env)->SetByteArrayRegion(env, result, 0, e->uncompressedSize, (jbyte *)e->data);
    } else {
        // uncompress it
#ifdef USE_CRITICAL_FOR_RETRIEVAL
        jboolean isCopy = 0;
        void *tmp = (*env)->GetPrimitiveArrayCritical(env, result, &isCopy);
        if (!tmp) {
            throwOutOfMemory(env);
            // TODO: release result?
            return result;
        }
        LZ4_decompress_fast(e->data, tmp, e->uncompressedSize);
        (*env)->ReleasePrimitiveArrayCritical(env, result, tmp, 0);  // transfer back data and release tmp buffer
#else
        // TODO: can the temporary buffer be avoided, i.e. we write directly into the jbyteArray buffer? It would skip 1 malloc / free plus an array copy
        char *tmp = malloc(e->uncompressedSize);
        if (!tmp) {
            throwOutOfMemory(env);
            // TODO: release result?
            return result;
        }
        LZ4_decompress_fast(e->data, tmp, e->uncompressedSize);
        (*env)->SetByteArrayRegion(env, result, 0, e->uncompressedSize, (jbyte *)tmp);
        free(tmp);
#endif
    }
    return result;
}


struct entry *find_entry(struct map *mapdata, jlong key) {
    int hash = computeHash(key, mapdata->hashTableSize);
    struct entry *e = mapdata->data[hash];
    while (e) {
        // check if this is a match
        if (e->key == key)
            return e;
        e = e->nextSameHash;
    }
    return e;  // null
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natGet
 * Signature: (JJ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_natGet
    (JNIEnv *env, jobject me, jlong cMap, jlong key) {
    struct map *mapdata = (struct map *) cMap;
    struct entry *e = find_entry(mapdata, key);
    return toJavaByteArray(env, e);
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natLength
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_natLength
    (JNIEnv *env, jobject me, jlong cMap, jlong key) {
    struct map *mapdata = (struct map *) cMap;
    struct entry *e = find_entry(mapdata, key);
    return (jint)(e ? e->uncompressedSize : -1);
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natCompressedLength
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_natCompressedLength
    (JNIEnv *env, jobject me, jlong cMap, jlong key) {
    struct map *mapdata = (struct map *) cMap;
    struct entry *e = find_entry(mapdata, key);
    return (jint)(e ? e->compressedSize : -1);
}



// remove an entry for a key. If transactions are active, redo log / rollback info will be stored. Else the entry no longer required will be freed.
jboolean execRemove(struct tx_log_hdr *ctx, struct map *mapdata, jlong key) {
    int hash = computeHash(key, mapdata->hashTableSize);
    struct entry *prev = NULL;
    struct entry *e = mapdata->data[hash];
    while (e) {
        // check if this is a match
        if (e->key == key) {
            if (!prev) {
                // initial entry, update mapdata
                mapdata->data[hash] = e->nextSameHash;
            } else {
                prev->nextSameHash = e->nextSameHash;
            }
#ifdef DEBUG
            fprintf(stderr, "Removing an entry of key %ld in slot %d\n", (long)key, hash);
#endif
            record_change(ctx, mapdata, e, NULL);
            --mapdata->count;
            return JNI_TRUE;
        }
        prev = e;
        e = e->nextSameHash;
    }
    // not found. No change of size
#ifdef DEBUG
    fprintf(stderr, "Not removing an entry of key %ld in slot %d (does not exist)\n", (long)key, hash);
#endif
    return JNI_FALSE;
}

// remove an entry for a key. If transactions are active, redo log / rollback info will be stored. Else the entry no longer required will be freed.
jboolean execRemoveShadow(struct map *mapdata, jlong key) {
    int hash = computeHash(key, mapdata->hashTableSize);
    struct entry *prev = NULL;
    struct entry *e = mapdata->data[hash];
    while (e) {
        // check if this is a match
        if (e->key == key) {
            if (!prev) {
                // initial entry, update mapdata
                mapdata->data[hash] = e->nextInCommittedView;
            } else {
                prev->nextSameHash = e->nextInCommittedView;
            }
#ifdef DEBUG
            fprintf(stderr, "Removing a shadow entry of key %ld in slot %d\n", (long)key, hash);
#endif
            free(e);
            --mapdata->count;
            return JNI_TRUE;
        }
        prev = e;
        e = e->nextInCommittedView;
    }
    // not found. No change of size
#ifdef DEBUG
    fprintf(stderr, "Not removing a shadow entry of key %ld in slot %d (does not exist)\n", (long)key, hash);
#endif
    return JNI_FALSE;
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natDelete
 * Signature: (JJJ)Z
 */
JNIEXPORT jboolean JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natDelete
    (JNIEnv *env, jobject me, jlong cMap, jlong ctx, jlong key) {
    return execRemove((struct tx_log_hdr *)ctx, (struct map *) cMap, key);
}


/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natRemove
 * Signature: (JJJ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natRemove
    (JNIEnv *env, jobject me, jlong cMap, jlong ctx, jlong key) {
    struct map *mapdata = (struct map *) cMap;
    int hash = computeHash(key, mapdata->hashTableSize);
    struct entry *prev = NULL;
    struct entry *e = mapdata->data[hash];
    while (e) {
        // check if this is a match
        if (e->key == key) {
            if (!prev) {
                // initial entry, update mapdata
                mapdata->data[hash] = e->nextSameHash;
            } else {
                prev->nextSameHash = e->nextSameHash;
            }
            jbyteArray result = toJavaByteArray(env, e);
            record_change((struct tx_log_hdr *)ctx, mapdata, e, NULL);
            --mapdata->count;
            return result;
        }
        prev = e;
        e = e->nextSameHash;
    }
    return (jbyteArray)0;
}


struct entry *create_new_entry(JNIEnv *env, jlong key, jbyteArray data, jint offset, jint length, jboolean doCompress) {
    // int uncompressed_length = (*env)->GetArrayLength(env, data);
    struct entry *e;
    if (doCompress) {
        // compress the data. Allocate a target buffer
        int max_compressed_length = ROUND_UP_SIZE(LZ4_compressBound(length));
        struct entry *tmp_dst = malloc(sizeof(struct entry) + max_compressed_length);
        if (!tmp_dst) {
            return NULL;  // will throw OOM
        }
#ifndef USE_CRITICAL_FOR_STORE
        void *tmp_src = malloc(length);
        if (!tmp_src) {
            free(tmp_dst);
            return NULL;  // will throw OOM
        }
        (*env)->GetByteArrayRegion(env, data, offset, length, tmp_src);
        int actual_compressed_length = LZ4_compress(tmp_src, tmp_dst->data, uncompressed_length);
        free(tmp_src);
#else
        // get the original array location, to avoid an extra copy
        jboolean isCopy = 0;
        void *src = (*env)->GetPrimitiveArrayCritical(env, data, &isCopy);
        if (!src) {
            free(tmp_dst);
            throwOutOfMemory(env);
            return NULL;
        }
        int actual_compressed_length = LZ4_compress(src+offset, tmp_dst->data, length);
        (*env)->ReleasePrimitiveArrayCritical(env, data, src, JNI_ABORT);  // abort, as we did not change anything
#endif
        // TODO: if the uncompressed size needs the same space (or less) than the compressed, use the uncompressed form instead!
        int actual_rounded_length = ROUND_UP_SIZE(actual_compressed_length);
        // if we need less than initially allocated, realloc to free the space.
        if (actual_rounded_length < max_compressed_length) {
            e = realloc(tmp_dst, sizeof(struct entry) + actual_rounded_length);
            if (!e)
                e = tmp_dst;
        } else {
            e = tmp_dst;
        }
        e->compressedSize = actual_compressed_length;
    } else {
        e = (struct entry *)malloc(sizeof(struct entry) + ROUND_UP_SIZE(length));
        if (!e)
            return NULL;  // will throw OOM
        e->compressedSize = 0;
        (*env)->GetByteArrayRegion(env, data, offset, length, (jbyte *)e->data);
    }
    e->uncompressedSize = length;
    e->nextInCommittedView = NULL;
    e->key = key;
    return e;
}


struct entry * setPutSub(struct map *mapdata, struct entry *newEntry) {
    jlong key = newEntry->key;
    int hash = computeHash(key, mapdata->hashTableSize);
    struct entry *e = mapdata->data[hash];
    newEntry->nextSameHash = e;  // insert it at the start
    newEntry->key = key;
    mapdata->data[hash] = newEntry;
    struct entry *prev = newEntry;

    while (e) {
        // check if this is a match
        if (e->key == key) {
            // replace that entry by the new one
            prev->nextSameHash = e->nextSameHash;
#ifdef DEBUG
            fprintf(stderr, "Replacing an entry of key %ld in slot %d\n", (long)key, hash);
#endif
            return e;
        }
        prev = e;
        e = e->nextSameHash;
    }
    // this is a new entry
#ifdef DEBUG
    fprintf(stderr, "Inserting an entry of key %ld in slot %d\n", (long)key, hash);
#endif
    ++mapdata->count;
    return NULL;
}
struct entry * setPutSubShadow(struct map *mapdata, struct entry *newEntry) {
    jlong key = newEntry->key;
    int hash = computeHash(key, mapdata->hashTableSize);
    struct entry *e = mapdata->data[hash];
    newEntry->nextInCommittedView = e;  // insert it at the start
    newEntry->key = key;
    mapdata->data[hash] = newEntry;
    struct entry *prev = newEntry;

    while (e) {
        // check if this is a match
        if (e->key == key) {
            // replace that entry by the new one
            prev->nextInCommittedView = e->nextInCommittedView;
#ifdef DEBUG
            fprintf(stderr, "Replacing a shadow entry of key %ld in slot %d\n", (long)key, hash);
#endif
            return e;
        }
        prev = e;
        e = e->nextInCommittedView;
    }
    // this is a new entry
#ifdef DEBUG
    fprintf(stderr, "Inserting a shadow entry of key %ld in slot %d\n", (long)key, hash);
#endif
    ++mapdata->count;
    return NULL;
}


/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natSet
 * Signature: (JJJ[BIIZ)Z
 */
JNIEXPORT jboolean JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natSet
    (JNIEnv *env, jobject me, jlong cMap, jlong ctx, jlong key, jbyteArray data, jint offset, jint length, jboolean doCompress) {
    struct map *mapdata = (struct map *) cMap;
    struct entry *newEntry = create_new_entry(env, key, data, offset, length, doCompress);
    if (!newEntry) {
        throwOutOfMemory(env);
        return JNI_FALSE;
    }

    struct entry *previousEntry = setPutSub(mapdata, newEntry);
    record_change((struct tx_log_hdr *)ctx, mapdata, previousEntry, newEntry);
    return (previousEntry == NULL);  // true if it was a new entry
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natPut
 * Signature: (JJJ[BIIZ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natPut
    (JNIEnv *env, jobject me, jlong cMap, jlong ctx, jlong key, jbyteArray data, jint offset, jint length, jboolean doCompress) {
    struct map *mapdata = (struct map *) cMap;
    struct entry *newEntry = create_new_entry(env, key, data, offset, length, doCompress);
    if (!newEntry) {
        throwOutOfMemory(env);
        return NULL;
    }

    struct entry *previousEntry = setPutSub(mapdata, newEntry);
    jbyteArray result = toJavaByteArray(env, previousEntry);
    record_change((struct tx_log_hdr *)ctx, mapdata, previousEntry, newEntry);
    return result;
}


int computeChainLength(struct entry *e) {
    register int len = 0;
    while (e) {
        ++len;
        e = e->nextSameHash;
    }
    return len;
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natGetHistogram
 * Signature: (J[I)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_natGetHistogram
    (JNIEnv *env, jobject me, jlong cMap, jintArray histogram) {
    struct map *mapdata = (struct map *) cMap;
    int maxLen = 0;
    int numHistogramEntries = (*env)->GetArrayLength(env, histogram);
    int *ctr = calloc(numHistogramEntries, sizeof(int));
    if (!ctr) {
        throwOutOfMemory(env);
        return -1;
    }
    int i;
    for (i = 0; i < mapdata->hashTableSize; ++i) {
        int len = computeChainLength(mapdata->data[i]);
        if (len > maxLen)
            maxLen = len;
        if (len < numHistogramEntries)
            ++ctr[len];
    }
    (*env)->SetIntArrayRegion(env, histogram, 0, numHistogramEntries, (jint *)ctr);
    free(ctr);
    return maxLen;
}


/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natGetIntoPreallocated
 * Signature: (JJ[BI)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_natGetIntoPreallocated
  (JNIEnv *env, jobject me, jlong cMap, jlong key, jbyteArray target, jint offset) {
    struct map *mapdata = (struct map *) cMap;
    struct entry *e = find_entry(mapdata, key);
    if (!e)
        return -1;  // key does not exist
    int targetSize = (*env)->GetArrayLength(env, target);

    if (offset >= targetSize)
        return 0;   // offset too big, no data will be copied
    if (e->compressedSize) {
        throwAny(env, "Copying from compressed entries not yet implemented");
        return -2;  // not yet implemented
    }
    int length = (offset + e->uncompressedSize > targetSize) ? targetSize - offset : e->uncompressedSize;
    (*env)->SetByteArrayRegion(env, target, offset, length, (jbyte *)e->data);
    return length;
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natGetRegion
 * Signature: (JJII)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_natGetRegion
  (JNIEnv *env, jobject me, jlong cMap, jlong key, jint offset, jint length) {
    struct map *mapdata = (struct map *) cMap;
    struct entry *e = find_entry(mapdata, key);
    if (!e)
        return (jbyteArray)0;
    if (e->compressedSize) {
        throwAny(env, "Copying from compressed entries not yet implemented");
        return (jbyteArray)0;  // not yet implemented
    }
    int len = offset >= e->uncompressedSize ? 0 : offset + length <= e->uncompressedSize ? length : e->uncompressedSize - offset;
    jbyteArray result = (*env)->NewByteArray(env, len);
    if (len)
        (*env)->SetByteArrayRegion(env, result, 0, len, (jbyte *)e->data + offset);
    return result;
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natGetField
 * Signature: (JJIBB)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_natGetField
  (JNIEnv *env, jobject me, jlong cMap, jlong key, jint fieldNo, jbyte delimiter, jbyte nullIndicator) {
    struct map *mapdata = (struct map *) cMap;
    struct entry *e = find_entry(mapdata, key);
    if (!e)
        return (jbyteArray)0;
    if (e->compressedSize) {
        throwAny(env, "Copying from compressed entries not yet implemented");
        return (jbyteArray)0;  // not yet implemented
    }
    char *ptr = e->data;
    char *startOfField = e->data;
    int bytesLeft = e->uncompressedSize;
    // search for start of field, until either bytesLeft are exhausted, or
    while (bytesLeft && fieldNo) {
        if (*ptr == delimiter) {
            --fieldNo;
            startOfField = ++ptr;
        } else if (*ptr == nullIndicator) {
//            if (fieldNo == 1)
//                return (jbyteArray)0;       // found it, it was 0!
            --fieldNo;
            startOfField = ++ptr;
        } else {
            // any other character
            ++ptr;
        }
        --bytesLeft;
    }
    if (!bytesLeft) {
        if (fieldNo) {
            // no more bytes, we field is out of bounds. Return null
            return (jbyteArray)0;
        }
        // here: fall through! We are the the field we are looking for, and at the same time at end of message
    } else {
        // found the field, and there are bytes left!
        // scan up to the next delimiter, or next null token, or end of record
        if (*ptr != delimiter && *ptr == nullIndicator) {
            return (jbyteArray)0;       // explicit NULL field
        }
        while (bytesLeft && *ptr != delimiter && *ptr != nullIndicator) {
            ++ptr;
            --bytesLeft;
        }
    }
    int len = ptr - startOfField;
    // the requested field was the last one in the object. We return a new byte array
    jbyteArray result = (*env)->NewByteArray(env, len);
    if (len)
        (*env)->SetByteArrayRegion(env, result, 0, len, (jbyte *) startOfField);
    return result;
}



struct filedumpHeader {
    int magicNumber;
    int numberOfRecords;
    long totalSize;
    jlong lastCommittedRef;
};


// buffer size calculation:
// we want to achieve the magnitide of the advertised 500 MB / s write speed.
// SSD write latency is about 3 ms (333 ops / second), or a buffer size of 1.5 MB.
// let's allocate about 2 megs then.  This just happens to be a hugepage on x86_64.
// all entries in the file will be 8-byte aligned, padded by 0xee
#define FILEDUMP_BUFFER_SIZE    (2 * 1024 * 1024)

// upon input, [0, FILEDUMP_BUFFER_SIZE] bytes are occupied, i.e. the first iteration possibly only writes 0 bytes
int transferWrite(const int fd, char *buffer, int oldOffset, void const *src, int len) {
    while (len > 0) {
        if (oldOffset + len < FILEDUMP_BUFFER_SIZE) {
            // copy all and, still space left
            memcpy(buffer + oldOffset, src, len);
            // possibly align by multipes of 8
            if (len & 7) {
                char *p = buffer+oldOffset+len;
                do {
                    *p++ = 0xee;
                } while (++len & 7);
            }
            return oldOffset + len;
        }
        // copy data and we reach end of buffer with that. Flush the buffer and loop
        int portion = FILEDUMP_BUFFER_SIZE - oldOffset;
        memcpy(buffer + oldOffset, src, portion);
        write(fd, buffer, FILEDUMP_BUFFER_SIZE);  // TODO error check later
        src += portion;
        oldOffset = 0;
        len -= portion;
    }
    return oldOffset;
}

char *allocateBuffers(JNIEnv *env, char **buffer, jbyteArray filename) {

#ifdef _ISOC11_SOURCE
    char *buffer = aligned_alloc(4 * 1024, FILEDUMP_BUFFER_SIZE);  // needs _ISOC11_SOURCE
#else
    *buffer = malloc(FILEDUMP_BUFFER_SIZE);
#endif
    if (!*buffer) {
        throwOutOfMemory(env);
        return NULL;
    }
    int filenameLen = (*env)->GetArrayLength(env, filename);
    char *filenameBuffer = malloc(filenameLen+1);
    if (!filenameBuffer) {
        free(buffer);
        throwOutOfMemory(env);
        return NULL;
    }
    (*env)->GetByteArrayRegion(env, filename, 0, filenameLen, (jbyte *)filenameBuffer);
    filenameBuffer[filenameLen] = 0;
    return filenameBuffer;
}

#define MAGIC_DB_CONSTANT   0x283462FE
#define ENTRY_HDR_SIZE      (2 * sizeof(int) + sizeof(jlong))


/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natWriteToFile
 * Signature: (J[BZ)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natWriteToFile
  (JNIEnv *env, jobject me, jlong cMap, jbyteArray filename, jboolean fromCommittedView) {
    struct filedumpHeader hdr;
    struct map *mapdata = (struct map *) cMap;
    char *buffer;

    char *filenameBuffer = allocateBuffers(env, &buffer, filename);
    if (!filenameBuffer)
        return;  // error
    int fd = open(filenameBuffer, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    free(filenameBuffer);
    if (fd < 0) {
        free(buffer);
        throwAny(env, "Cannot open file");
        return;
    }

    if (!mapdata->committedView)
        fromCommittedView = JNI_FALSE;
    if (fromCommittedView)
        mapdata = mapdata->committedView;

    // transfer header
    hdr.magicNumber = MAGIC_DB_CONSTANT;
    hdr.numberOfRecords = mapdata->count;
    hdr.lastCommittedRef = mapdata->lastCommittedRef;
    hdr.totalSize = 0L;  // findSize(mapdata)

    int bufferOffset = transferWrite(fd, buffer, 0, &hdr, sizeof(hdr));
    // write the entries
    int i;
    for (i = 0; i < mapdata->hashTableSize; ++i) {
        struct entry *e;
        for (e = mapdata->data[i]; e; e = (fromCommittedView ? e->nextInCommittedView : e->nextSameHash)) {
            int rawSize = e->compressedSize ? e->compressedSize : e->uncompressedSize;
            int finalSize = 2 * sizeof(int) + sizeof(jlong) + rawSize;
            bufferOffset = transferWrite(fd, buffer, bufferOffset, &(e->uncompressedSize), finalSize);
        }
    }

    if (bufferOffset) {
        write(fd, buffer, bufferOffset);
    }
    close(fd);
    free(buffer);
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natReadFromFile
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natReadFromFile
  (JNIEnv *env, jobject me, jlong cMap, jbyteArray filename) {
    struct filedumpHeader hdr;
    struct map *mapdata = (struct map *) cMap;

    if (mapdata->count) {
        throwAny(env, "DB is not empty");
        return;
    }

    char *buffer;

    char *filenameBuffer = allocateBuffers(env, &buffer, filename);
    if (!filenameBuffer)
        return;  // error

    // use buffered I/O for reading.
    FILE *fp = fopen(filenameBuffer, "rb");
    free(filenameBuffer);
    if (!fp) {
        throwAny(env, "Cannot open file");
        free(buffer);
        return;
    }

    setvbuf(fp, buffer, _IOFBF, FILEDUMP_BUFFER_SIZE);

    if (fread(&hdr, sizeof(hdr), 1, fp) != 1) {
        throwAny(env, "Cannot read file header");
        free(buffer);
        fclose(fp);
        return;
    }
    if (hdr.magicNumber != MAGIC_DB_CONSTANT) {
        throwAny(env, "File is not a DB file (bad magic number)");
        free(buffer);
        fclose(fp);
        return;
    }

    int i;
    for (i = 0; i < hdr.numberOfRecords; ++i) {
        // read the entry header: key, uncompressed & compressed size
        struct entry entryHdr;
        if (fread(&(entryHdr.uncompressedSize), ENTRY_HDR_SIZE, 1, fp) != 1) {
            free(buffer);
            fclose(fp);
            throwAny(env, "Cannot read entry header");
            return;
        }
        int actualSize = entryHdr.compressedSize ? entryHdr.compressedSize : entryHdr.uncompressedSize;
        struct entry *e = malloc(sizeof(struct entry) + ROUND_UP_SIZE(actualSize));
        if (!e) {
            free(buffer);
            fclose(fp);
            throwOutOfMemory(env);
        }
        e->key = entryHdr.key;
        e->uncompressedSize = entryHdr.uncompressedSize;
        e->compressedSize = entryHdr.compressedSize;

        int hash = computeHash(entryHdr.key, mapdata->hashTableSize);
        e->nextInCommittedView = e->nextSameHash = mapdata->data[hash];
        mapdata->data[hash] = e;
        if (fread(e->data, ROUND_UP_FILESIZE(actualSize), 1, fp) != 1) {
            free(buffer);
            fclose(fp);
            throwAny(env, "Cannot read entry data");
            return;
        }
        ++mapdata->count;
    }
    free(buffer);
    fclose(fp);

    // mapdata->count = hdr.numberOfRecords;
    mapdata->lastCommittedRef = hdr.lastCommittedRef;
    struct map *viewdata = mapdata->committedView;
    if (viewdata) {
        // transfer everything from main view to committed view as well
        // assumes same hashtablesize
        memcpy(viewdata->data, mapdata->data, sizeof(struct entry *) * mapdata->hashTableSize);
        viewdata->count = mapdata->count;
        viewdata->lastCommittedRef = mapdata->lastCommittedRef;
    }
}
