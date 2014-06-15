#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jni.h>
#include <lz4.h>
#include "jpawMap.h"

#define USE_CRITICAL_FOR_STORE
#define USE_CRITICAL_FOR_RETRIEVAL

#undef SEPARATE_COMMITTED_VIEW

#undef DEBUG
//#define DEBUG

struct entry {
    int uncompressedSize;
    int compressedSize;  // 0 = is not compressed
    jlong key;
    struct entry *nextSameHash;
#ifdef SEPARATE_COMMITTED_VIEW
    struct entry *nextInCommittedView;
#endif
    char data[];
};

struct map {
    int hashTableSize;
    int modes;
    int count;              // current number of entries. tracking this in Java gives a faster size() operation, but causes problems (callback required) for rollback
    int padding;
    struct entry **data;
#ifdef SEPARATE_COMMITTED_VIEW
    struct entry **committedViewData;
#endif
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

#define TX_LOG_ENTRIES_PER_CHUNK_LV1    1024        // first level blocks
#define TX_LOG_ENTRIES_PER_CHUNK_LV2     256        // changes in final block


// transaction modes. bitwise ORed, they go into the modes field of current_transaction

#define TRANSACTIONAL 0x01     // allow to rollback / safepoints
#define REDOLOG_ASYNC 0x02     // allow replaying on a different database - fast
#define REDOLOG_SYNC 0x04      // allow replaying on a different database - safe
#define AS_PER_TRANSACTION (-1)   // no override in map

struct tx_log_list {
    struct tx_log_entry entries[TX_LOG_ENTRIES_PER_CHUNK_LV2];
};

// global variables
struct tx_log_hdr {
    int number_of_changes;
    int modes;
    struct tx_log_list *chunks[TX_LOG_ENTRIES_PER_CHUNK_LV1];
};
static jclass javaTxClass;
static jfieldID javaTxCStructFID;
static jclass javaMapClass;
static jfieldID javaMapCStructFID;


// reference: see http://www3.ntu.edu.sg/home/ehchua/programming/java/JavaNativeInterface.html


// round the size to be allocated to the next multiple of 16, because malloc anyway returns multiples of 16.
int round_up_size(int size) {
    return ((size - 1) & ~0x0f) + 16;
}

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
struct entry * setPutSub(struct map *mapdata, struct entry *newEntry);


// transactions

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natInit
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natInit
  (JNIEnv *env, jclass txClass) {
    javaTxClass = (*env)->NewGlobalRef(env, txClass); // call to newGlobalRef is required because otherwise jclass is only valid for the current call

    // Get the Field ID of the instance variables "number"
    javaTxCStructFID = (*env)->GetFieldID(env, javaTxClass, "cStruct", "J");
    if (!javaTxCStructFID) {
        throwAny(env, "Invoking class must have a field long cStruct");
        return;
    }
}

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCreateTransaction
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCreateTransaction
  (JNIEnv *env, jobject tx, jint mode) {
    struct tx_log_hdr *hdr = malloc(sizeof(struct tx_log_hdr));
    if (!hdr) {
        throwOutOfMemory(env);
        return (jlong)0;
    }
    memset(hdr, 0, sizeof(struct tx_log_hdr));
    hdr->modes = mode;
    hdr->number_of_changes = 0;
#ifdef DEBUG
    fprintf(stderr, "CREATE TRANSACTION\n");
#endif
    return (jlong)hdr;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCloseTransaction
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCloseTransaction
  (JNIEnv *env, jobject me) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) ((*env)->GetLongField(env, me, javaTxCStructFID));
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
    (*env)->SetLongField(env, me, javaTxCStructFID, (jlong)0);
#ifdef DEBUG
    fprintf(stderr, "CLOSE TRANSACTION\n");
#endif
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natSetMode
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natSetMode
  (JNIEnv *env, jobject me, jint mode) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) ((*env)->GetLongField(env, me, javaTxCStructFID));
    if (hdr->number_of_changes) {
        throwAny(env, "Cannot change mode within pending transaction");
        return;
    }
    hdr->modes = mode;
}

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natSetSafepoint
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natSetSafepoint
  (JNIEnv *env, jobject me) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) ((*env)->GetLongField(env, me, javaTxCStructFID));
    return hdr->number_of_changes;
}

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCommit
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCommit
  (JNIEnv *env, jobject me, jlong ref) {
    // currently no redo logs are written. Therefore just discard the entries
#ifdef DEBUG
    fprintf(stderr, "COMMIT START\n");
#endif
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) ((*env)->GetLongField(env, me, javaTxCStructFID));
    int currentEntries = hdr->number_of_changes;
    if (currentEntries) {
        struct tx_log_list *chunk;
        int i;
        for (i = 0; i < currentEntries; ++i) {
            if (!(i & 0xff)) {
                // need a new chunk
                chunk = hdr->chunks[i >> 8];
            }
            struct entry *e = chunk->entries[i & 0xff].old_entry;
            if (e)
                free(e);
        }
    }
    hdr->number_of_changes = 0;
#ifdef DEBUG
    fprintf(stderr, "COMMIT END\n");
#endif
    return currentEntries;
}

void rollback(struct tx_log_entry *e) {
#ifdef DEBUG
    fprintf(stderr, "Rolling back entry for %016p %016p %016p\n", e->affected_table, e->old_entry, e->new_entry);
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
        fprintf(stderr, "    => insert / update %016p with key %ld\n", e->old_entry, e->old_entry->key);
#endif
        struct entry *shouldBeNew = setPutSub(e->affected_table, e->old_entry);  // TODO: assert we got the new entry (NULL or not)
        if (shouldBeNew != e->new_entry)
            fprintf(stderr, "ROLLBACK PROBLEM: expected to get %016p, but got %016p for key %ld\n", e->new_entry, shouldBeNew, e->old_entry->key);
        // if new_entry was not null, then free it (it is no longer required)
        if (e->new_entry)
            free(e->new_entry);
    }
}


/** Record a row change. Returns an error if anything went wrong. */
char *record_change(struct tx_log_hdr *ctx, struct map *mapdata, struct entry *oldData, struct entry *newData) {
    if (!ctx || mapdata->modes == 0 || !ctx->modes) {
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
    fprintf(stderr, "Storing rollback data at %016p: map=%016p old=%016p new=%016p\n", loge, mapdata, oldData, newData);
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
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natRollback
  (JNIEnv *env, jobject me, jint rollbackTo) {
#ifdef DEBUG
    fprintf(stderr, "ROLLBACK START (%d)\n", rollbackTo);
#endif
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) ((*env)->GetLongField(env, me, javaTxCStructFID));
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
 * Class:     Java_de_jpaw_offHeap_OffHeapTransaction_natInit
 * Method:    natInit
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natInit(JNIEnv *env, jobject me) {
    javaMapClass = (*env)->NewGlobalRef(env, (*env)->GetObjectClass(env, me)); // call to newGlobalRef is required because otherwise jclass is only valid for the current call

    // Get the Field ID of the instance variables "number"
    javaMapCStructFID = (*env)->GetFieldID(env, javaMapClass, "cStruct", "J");
    if (!javaMapCStructFID) {
        throwAny(env, "Invoking class must have a field long cStruct");
        return;
    }
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natOpen
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natOpen(JNIEnv *env, jobject me, jint size, jint mode) {
    // round up the size to multiples of 32, for the collision indicator
    size = ((size - 1) | 31) + 1;
    struct map *mapdata = malloc(sizeof(struct map));
    if (!mapdata) {
        throwOutOfMemory(env);
        return;
    }
    mapdata->hashTableSize = size;
    mapdata->modes = mode;
    mapdata->data = calloc(size, sizeof(struct entry *));
    mapdata->count = 0;
    mapdata->padding = 0;
    if (!mapdata->data) {
        throwOutOfMemory(env);
        return;
    }

    // printf("jpawMap: created new map at %p\n", mapdata);
    (*env)->SetLongField(env, me, javaMapCStructFID, (jlong) mapdata);
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
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natClose
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natClose(JNIEnv *env, jobject me) {
    // Get the int given the Field ID
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
    // printf("jpawMap: closing map at %p\n", mapdata);
    clear(mapdata->data, mapdata->hashTableSize);
    free(mapdata->data);
    free(mapdata);
    (*env)->SetLongField(env, me, javaMapCStructFID, (jlong) 0);
    // do not delete it, it could be usedby other maps
//    (*env)->DeleteGlobalRef(env, javaMapClass);
//    javaMapClass = NULL;
}


/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natGetSize
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natGetSize
  (JNIEnv *env, jobject me) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
    return mapdata->count;
}


/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natClear
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natClear(JNIEnv *env, jobject me) {
    // Get the int given the Field ID
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
    clear(mapdata->data, mapdata->hashTableSize);
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


struct entry *find_entry(struct map *mapdata, long key) {
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
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natGet
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natGet(JNIEnv *env, jobject me, jlong key) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
    struct entry *e = find_entry(mapdata, key);
    return toJavaByteArray(env, e);
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natLength
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natLength(JNIEnv *env, jobject me, jlong key) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
    struct entry *e = find_entry(mapdata, key);
    return (jint)(e ? e->uncompressedSize : -1);
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natCompressedLength
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natCompressedLength(JNIEnv *env, jobject me, jlong key) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
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
            record_change(ctx, mapdata, e, NULL);
            --mapdata->count;
            return JNI_TRUE;
        }
        prev = e;
        e = e->nextSameHash;
    }
    // not found. No change of size
    return JNI_FALSE;
}


/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natDelete
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natDelete(JNIEnv *env, jobject me, jlong ctx, jlong key) {
    return execRemove((struct tx_log_hdr *)ctx, (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID)), key);
}


/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natRemove
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natRemove(JNIEnv *env, jobject me, jlong ctx, jlong key) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
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


struct entry *create_new_entry(JNIEnv *env, jlong key, jbyteArray data, jboolean doCompress) {
    int uncompressed_length = (*env)->GetArrayLength(env, data);
    struct entry *e;
    if (doCompress) {
        // compress the data. Allocate a target buffer
        int max_compressed_length = round_up_size(LZ4_compressBound(uncompressed_length));
        struct entry *tmp_dst = malloc(sizeof(struct entry) + max_compressed_length);
        if (!tmp_dst) {
            return NULL;  // will throw OOM
        }
#ifndef USE_CRITICAL_FOR_STORE
        void *tmp_src = malloc(uncompressed_length);
        if (!tmp_src) {
            free(tmp_dst);
            return NULL;  // will throw OOM
        }
        (*env)->GetByteArrayRegion(env, data, 0, uncompressed_length, tmp_src);
        int actual_compressed_length = LZ4_compress(tmp_src, tmp_dst->data, uncompressed_length);
        free(tmp_src);
#else
        // get the original array location, to avoid an extra copy
        jboolean isCopy = 0;
        void *src = (*env)->GetPrimitiveArrayCritical(env, data, &isCopy);
        if (!src) {
            throwOutOfMemory(env);
            // TODO: release result?
            return NULL;
        }
        int actual_compressed_length = LZ4_compress(src, tmp_dst->data, uncompressed_length);
        (*env)->ReleasePrimitiveArrayCritical(env, data, src, JNI_ABORT);  // abort, as we did not change anything
#endif
        // TODO: if the uncompressed size needs the same space (or less) than the compressed, use the uncompressed form instead!
        int actual_rounded_length = round_up_size(actual_compressed_length);
        // if we need less than initially allocated, realloc to free the space.
        if (actual_rounded_length < max_compressed_length) {
            e = realloc(tmp_dst, sizeof(struct entry) + actual_rounded_length);
            if (!e)
                e = tmp_dst;
        } else {
            e = tmp_dst;
        }
        e->uncompressedSize = uncompressed_length;
        e->compressedSize = actual_compressed_length;
    } else {
        e = (struct entry *)malloc(sizeof(struct entry) + round_up_size(uncompressed_length));
        if (!e)
            return NULL;  // will throw OOM
        e->uncompressedSize = uncompressed_length;
        e->compressedSize = 0;
        (*env)->GetByteArrayRegion(env, data, 0, uncompressed_length, (jbyte *)e->data);
    }
    e->key = key;
    return e;
}


struct entry * setPutSub(struct map *mapdata, struct entry *newEntry) {
//    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
//    struct entry *newEntry = create_new_entry(env, data, doCompress);
    int key = newEntry->key;
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
            return e;
        }
        prev = e;
        e = e->nextSameHash;
    }
    // this is a new entry
    ++mapdata->count;
    return NULL;
}


/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natSet
 * Signature: (J[BZ)Z
 */
JNIEXPORT jboolean JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natSet(
        JNIEnv *env, jobject me, jlong ctx, jlong key, jbyteArray data, jboolean doCompress) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
    struct entry *newEntry = create_new_entry(env, key, data, doCompress);
    if (!newEntry) {
        throwOutOfMemory(env);
        return JNI_FALSE;
    }

    struct entry *previousEntry = setPutSub(mapdata, newEntry);
    record_change((struct tx_log_hdr *)ctx, mapdata, previousEntry, newEntry);
    return (previousEntry == NULL);  // true if it was a new entry
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natPut
 * Signature: (J[BZ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natPut(JNIEnv *env, jobject me, jlong ctx, jlong key, jbyteArray data, jboolean doCompress) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
    struct entry *newEntry = create_new_entry(env, key, data, doCompress);
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
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natGetHistogram
 * Signature: ([I)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natGetHistogram
  (JNIEnv *env, jobject me, jintArray histogram) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
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
