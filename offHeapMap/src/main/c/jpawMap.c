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
    int filler;
    struct entry **data;
#ifdef SEPARATE_COMMITTED_VIEW
    struct entry **committedViewData;
#endif
};

// stores a single modification of the maps.
// the relevant types are insert / update / remove.
// The type can be determined by which ptr is null and which not.
// the "nextSameHash" entries are "misused"
struct transaction_log_entry {
    struct map *affected_table;
    struct entry *old_entry;
    struct entry *new_entry;
};

#define TX_LOG_ENTRIES_PER_CHUNK 168


// transaction modes. bitwise ORed, they go into the modes field of current_transaction

#define TRANSACTIONAL 0x01     // allow to rollback / safepoints
#define REDOLOG_ASYNC 0x02     // allow replaying on a different database - fast
#define REDOLOG_SYNC 0x04      // allow replaying on a different database - safe

// size of this is 8 + 168 * 24 = 4040 byte.
// It should fit into a single page, including some (up to 56 byte) malloc header
struct transaction_log_chunk {
    struct transaction_log_chunk *next_chunk;
    int num_used;
    int filler;  // padding to give it 8 byte boundary
    struct transaction_log_entry entries[TX_LOG_ENTRIES_PER_CHUNK];
};

// global variables
struct current_transaction {
    int number_of_changes;
    int modes;
    long transaction_ref;       // system change no
    struct transaction_log_chunk *logs;
} current_transaction = {
    0, 0, 0, 0, 0L, NULL
};
static jclass javaMapClass;
static jfieldID fidNumber;


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

// transactions



















/*
 * Class:     Java_de_jpaw_offHeap_OffHeapTransaction_natInit
 * Method:    natInit
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natInit(JNIEnv *env, jobject me) {
    javaMapClass = (*env)->NewGlobalRef(env, (*env)->GetObjectClass(env, me)); // call to newGlobalRef is required because otherwise jclass is only valid for the current call

    // Get the Field ID of the instance variables "number"
    fidNumber = (*env)->GetFieldID(env, javaMapClass, "cStruct", "J");
    if (!fidNumber) {
        throwAny(env, "Invoking class must have a field long cStruct");
        return;
    }
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natOpen
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natOpen(JNIEnv *env, jobject me, jint size) {
    // round up the size to multiples of 32, for the collision indicator
    size = ((size - 1) | 31) + 1;
    struct map *mapdata = malloc(sizeof(struct map));
    if (!mapdata) {
        throwOutOfMemory(env);
        return;
    }
    mapdata->hashTableSize = size;
    mapdata->data = calloc(size, sizeof(struct entry *));
    if (!mapdata->data) {
        throwOutOfMemory(env);
        return;
    }

    // printf("jpawMap: created new map at %p\n", mapdata);
    (*env)->SetLongField(env, me, fidNumber, (jlong) mapdata);
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
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, fidNumber));
    // printf("jpawMap: closing map at %p\n", mapdata);
    clear(mapdata->data, mapdata->hashTableSize);
    free(mapdata->data);
    free(mapdata);
    (*env)->SetLongField(env, me, fidNumber, (jlong) 0);
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
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, fidNumber));
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
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, fidNumber));
    struct entry *e = find_entry(mapdata, key);
    return toJavaByteArray(env, e);
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natLength
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natLength(JNIEnv *env, jobject me, jlong key) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, fidNumber));
    struct entry *e = find_entry(mapdata, key);
    return (jint)(e ? e->uncompressedSize : -1);
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natRemove
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natRemove(JNIEnv *env, jobject me, jlong key) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, fidNumber));
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
            free(e);
            return JNI_TRUE;
        }
        prev = e;
        e = e->nextSameHash;
    }
    return JNI_FALSE;
}


/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natGetAndRemove
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natGetAndRemove(JNIEnv *env, jobject me, jlong key) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, fidNumber));
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
            free(e);
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


struct entry * setPutSub(JNIEnv *env, jobject me, jlong key, jbyteArray data, jboolean doCompress) {
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, fidNumber));
    int hash = computeHash(key, mapdata->hashTableSize);
    struct entry *newEntry = create_new_entry(env, data, doCompress);
    if (!newEntry) {
        throwOutOfMemory(env);
        return NULL;
    }
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
        JNIEnv *env, jobject me, jlong key, jbyteArray data, jboolean doCompress) {

    struct entry *previousEntry = setPutSub(env, me, key, data, doCompress);

    if (previousEntry != NULL) {
        free(previousEntry);
        return JNI_FALSE;
    }
    return JNI_TRUE;  // was a new entry
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natPut
 * Signature: (J[BZ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natPut(JNIEnv *env, jobject me, jlong key, jbyteArray data, jboolean doCompress) {
    struct entry *previousEntry = setPutSub(env, me, key, data, doCompress);

    if (previousEntry != NULL) {
        jbyteArray result = toJavaByteArray(env, previousEntry);
        free(previousEntry);
        return result;
    }
    return NULL;
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
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, fidNumber));
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
