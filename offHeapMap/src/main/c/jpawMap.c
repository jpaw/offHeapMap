#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jni.h>
#include <lz4.h>
#include "jpawMap.h"

#undef SEPARATE_COMMITTED_VIEW


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
//    long transaction_ref;       // system change no => set during commit()
//    struct tx_log_list *logsFirst;
//    struct tx_log_list *logsLast;
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
//    hdr->logsFirst = NULL;
//    hdr->logsLast = NULL;
    return (jlong)hdr;
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
 * Method:    natCommit
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCommit
  (JNIEnv *env, jobject me, jlong ref) {
    // currently no redo logs are written. Therefore just discard the entries
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
    return currentEntries;
}

void rollback(struct tx_log_entry *e) {
    if (!e->old_entry) {
        // was an insert
        execRemove(NULL, e->affected_table, e->old_entry->key); // TODO: assert new entry was returned
    } else {
        // replace or delete
        struct entry *shouldBeNew = setPutSub(e->affected_table, e->old_entry);  // TODO: assert we got the new entry (NULL or not)
    }
}


/** Record a row change. Returns an error if anything went wrong. */
char *record_change(struct tx_log_hdr *ctx, struct map *mapdata, struct entry *oldData, struct entry *newData) {
    if (!ctx || !mapdata->modes) {
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
    struct tx_log_entry *loge = &(chunk->entries[chunkIndex & 0xff]);
    loge->affected_table = mapdata;
    loge->old_entry = oldData;
    loge->new_entry = newData;
    ++ctx->number_of_changes;
    return NULL;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natRollback
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natRollback
  (JNIEnv *env, jobject me, jint rollbackTo) {
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
 * Method:    natClear
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natClear(JNIEnv *env, jobject me) {
    // Get the int given the Field ID
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID));
    clear(mapdata->data, mapdata->hashTableSize);
    memset(mapdata->data, 0, mapdata->hashTableSize * sizeof(struct entry *));     // set the initial pointers to NULL
}

static jbyteArray toJavaByteArray(JNIEnv *env, struct entry *e) {
    if (!e)
        return (jbyteArray) 0;
    jbyteArray result = (*env)->NewByteArray(env, e->uncompressedSize);
    if (!e->compressedSize) {
        (*env)->SetByteArrayRegion(env, result, 0, e->uncompressedSize, (jbyte *)e->data);
    } else {
        // uncompress it
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
            return JNI_TRUE;
        }
        prev = e;
        e = e->nextSameHash;
    }
    return JNI_FALSE;
}


/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natRemove
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natRemove(JNIEnv *env, jobject me, jlong ctx, jlong key) {
    return execRemove((struct tx_log_hdr *)ctx, (struct map *) ((*env)->GetLongField(env, me, javaMapCStructFID)), key);
}


/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natGetAndRemove
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natGetAndRemove(JNIEnv *env, jobject me, jlong ctx, jlong key) {
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
            return result;
        }
        prev = e;
        e = e->nextSameHash;
    }
    return (jbyteArray)0;
}


struct entry *create_new_entry(JNIEnv *env, jbyteArray data, jboolean doCompress) {
    int uncompressed_length = (*env)->GetArrayLength(env, data);
    struct entry *e;
    if (doCompress) {
        // compress the data
        char *tmp_src = malloc(uncompressed_length);
        if (!tmp_src)
            return NULL;  // will throw OOM
        (*env)->GetByteArrayRegion(env, data, 0, uncompressed_length, (jbyte *)tmp_src);
        int tmp_length = LZ4_compressBound(uncompressed_length);
        char *tmp_dst = malloc(tmp_length);
        if (!tmp_dst) {
            free(tmp_src);
            return NULL;  // will throw OOM
        }
        int actual_compressed_length = LZ4_compress(tmp_src, tmp_dst, uncompressed_length);
        free(tmp_src);
        e = (struct entry *)malloc(sizeof(struct entry) + round_up_size(actual_compressed_length));
        if (!e) {
            free(tmp_dst);
            return NULL;  // will throw OOM
        }
        e->uncompressedSize = uncompressed_length;
        e->compressedSize = actual_compressed_length;
        memcpy(e->data, tmp_dst, actual_compressed_length);
        free(tmp_dst);
    } else {
        e = (struct entry *)malloc(sizeof(struct entry) + round_up_size(uncompressed_length));
        if (!e)
            return NULL;  // will throw OOM
        e->uncompressedSize = uncompressed_length;
        e->compressedSize = 0;
        (*env)->GetByteArrayRegion(env, data, 0, uncompressed_length, (jbyte *)e->data);
    }
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
    struct entry *newEntry = create_new_entry(env, data, doCompress);
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
    struct entry *newEntry = create_new_entry(env, data, doCompress);
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
