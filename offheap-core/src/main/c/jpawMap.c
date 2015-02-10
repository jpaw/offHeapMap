#include "jpawMap.h"
#include "globalDefs.h"
#include "globalMethods.h"

#define USE_CRITICAL_FOR_STORE
// #define USE_CRITICAL_FOR_RETRIEVAL       // this seems to be much slower!


// round the size to be allocated to the next multiple of 16, because malloc anyway returns multiples of 16.
#define ROUND_UP_SIZE(size)     ((((size) - 1) & ~0x0f) + 16)
#define ROUND_UP_FILESIZE(size) ((((size) - 1) & ~0x07) + 8)  // as stored in the disk dump file

struct dataEntry {
    // start with the internal ptr, to allow writing a continuous area when dumping the file.
    struct dataEntry *nextSameHash;
#ifdef SEPARATE_COMMITTED_VIEW
    struct dataEntry *nextInCommittedView;
#endif
#ifdef ADD_INDEX
    struct dataEntry *nextIndex;
#ifdef SEPARATE_COMMITTED_VIEW
    struct dataEntry *nextIndexInCommittedView;
#endif
#endif
    // from here, the dataEntry is dumped to disk on saves.
    int uncompressedSize;       // size of the data, without this header portion. This could be 0, for some index types.
    int compressedSize;         // for data: 0 = is not compressed, otherwise size of the compressed output. The actual allocated space is rounded up such that the dataEntry size is always a multiple of 16
    jlong key;
    char data[];
};
// the field compressedSize contains 0 or the compressed size (in byte) for data maps. For index maps,
// it contains either the value (for byte, short, char, int; in this case uncompressed size is 0)
// or a hashcode of the value (any other key type), in this case uncompressed size is the size of the data block which contains the serialized form of the key.

// the hashCode (index of primary array) is for data maps the hash of the key, for index maps the hash of the index (modulus something)

// in both cases, a key occurs once only, for every map.


// overloaded methods
struct fctnPtrs {
    void (*commitToView)(struct tx_log_entry *ep, jlong transactionReference);
    void (*rollback)(struct tx_log_entry *ep);
    void (*print)(struct tx_log_entry *ep, int i);
};


// a map, as used for key to value lookups, but alos reverse lookups (index => key).
// the difference between data maps and index maps is the use of the hash to select the appropriate slot.
// for data lookup, it is based on the key, for index lookups, it is based on the hash (as stored in dataEntry->compressedSize field)
struct map {
    int mapType;                    // dataMap, indexMap, unique flag, organization, has view
    int count;                      // current number of entries. tracking this in Java gives a faster size() operation, but causes problems (callback required) for rollback
    int hashTableSize;
    int modes;                      // 00 = not transactional, 0x80 = take mode of transaction, 1 = TRANSACTIONAL.. see globalDefs.h
    struct dataEntry **keyHash;
    struct map *committedView;      // same data, but synched after commit (to provide secondary view for read/only queries, i.e. dirty read as well as committed read views...)
    jlong lastCommittedRef;
    void *sharedIndexLookupBuffer;  // for the singlethreaded dirty view: allows reusing temporary storage for index lookups
    int sharedIndexLookupBufferSize;
};


static jfieldID javaIteratorCurrentHashIndexFID;
static jfieldID javaIteratorCurrentKeyFID;
static jfieldID javaIndexIteratorCurrentKeyFID;
static jfieldID javaIndexIteratorCurrentSizeFID;



// other protos...
// static methods commented out..
//
//int computeKeyHash(jlong arg, int size);
//void clear(struct dataEntry **keyHash, int numEntries);
//int record_change(JNIEnv *env, struct tx_log_hdr *ctx, struct map *mapdata, struct dataEntry *oldData, struct dataEntry *newData);
//int transferWrite(const int fd, char *buffer, int oldOffset, void const *src, int len);
//char *allocateBuffers(JNIEnv *env, char **buffer, jbyteArray filename);
//struct dataEntry *find_entry(struct map *mapdata, jlong key);
//struct dataEntry *create_new_entry(JNIEnv *env, jlong key, jbyteArray data, jint offset, jint length, jboolean doCompress);
//jboolean execRemove(JNIEnv *env, struct tx_log_hdr *ctx, struct map *mapdata, jlong key);
//jboolean execRemoveShadow(struct map *mapdata, jlong key);
//struct dataEntry * setPutSub(struct map *mapdata, struct dataEntry *newEntry);
//struct dataEntry * setPutSubShadow(struct map *mapdata, struct dataEntry *newEntry);

void throwInconsistent(JNIEnv *env, char *msg) {
    jclass exceptionCls = (*env)->FindClass(env, "de/jpaw/collections/InconsistentIndexException");
    (*env)->ThrowNew(env, exceptionCls, msg);
}
void throwDuplicateKey(JNIEnv *env) {
    jclass exceptionCls = (*env)->FindClass(env, "de/jpaw/collections/DuplicateIndexException");
    (*env)->ThrowNew(env, exceptionCls, "index already exists");
}
void throwOutOfMemory(JNIEnv *env) {
    jclass exceptionCls = (*env)->FindClass(env, "java/lang/RuntimeException");
    (*env)->ThrowNew(env, exceptionCls, "Out of off-heap memory in JNI call");
}
void throwAny(JNIEnv *env, char *msg) {
    jclass exceptionCls = (*env)->FindClass(env, "java/lang/RuntimeException");
    (*env)->ThrowNew(env, exceptionCls, msg);
}



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
    struct dataEntry *e = (struct dataEntry *)nextEntryPtr;
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
        e = mapdata->keyHash[hashIndex];
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




/** Record a row change. Returns an error if anything went wrong. */
static int record_change(JNIEnv *env, struct tx_log_hdr *ctx, struct map *mapdata, struct dataEntry *oldData, struct dataEntry *newData) {
    if (!IS_TRANSACTIONAL(ctx, mapdata)) {
        // no transaction log. Maybe free old data
        if (oldData)
            free(oldData);
        return 0;
    }

    struct tx_log_entry *loge = getTxLogEntry(env, ctx);
    if (!loge)
        return 1;  // error
#ifdef DEBUG
    fprintf(stderr, "Storing rollback data %6d at %16p: map=%16p old=%16p new=%16p\n", ctx->number_of_changes, loge, mapdata, oldData, newData);
#endif
    loge->affected_table = mapdata;
    loge->old_entry = oldData;
    loge->new_entry = newData;
    ++(ctx->number_of_changes);

    if (!(ctx->number_of_changes & 0x3ff))
        fprintf(stderr, "WARNING: Transaction has grown to %d changes...\n", ctx->number_of_changes);
    return 0;
}

//
//
//
//      AbstractOffHeapMap methods....
//
//
//


/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natOpen
 * Signature: (IIZ)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_AbstractOffHeapMap_natOpen
    (JNIEnv *env, jobject me, jint size, jint mode, jboolean withCommittedView) {
    // round up the size to multiples of 32, for the collision indicator
    size = ((size - 1) | 31) + 1;
    struct map *mapdata = malloc(sizeof(struct map));
    if (!mapdata) {
        throwOutOfMemory(env);
        return 0L;
    }
    mapdata->mapType = MAP_TYPE_DATA_AND_VIEW;
    mapdata->count = 0;
    mapdata->hashTableSize = size;
    mapdata->modes = mode;
    mapdata->lastCommittedRef = -1L;
    mapdata->committedView = NULL;
    mapdata->keyHash = calloc(size, sizeof(struct dataEntry *));
    mapdata->sharedIndexLookupBufferSize = 0;
    mapdata->sharedIndexLookupBuffer = NULL;
    if (!mapdata->keyHash) {
        free(mapdata);
        throwOutOfMemory(env);
        return 0L;
    }
    if (withCommittedView) {
        struct map *view = malloc(sizeof(struct map));
        if (!view) {
            free(mapdata->keyHash);
            free(mapdata);
            throwOutOfMemory(env);
            return 0L;
        }
        memcpy(view, mapdata, sizeof(struct map));
        mapdata->committedView = view;

        view->modes = mode & VIEW_INDEX_MASK;        // the committed view does not have any TX management
        view->keyHash = calloc(size, sizeof(struct dataEntry *));
        if (!view->keyHash) {
            free(view);
            free(mapdata->keyHash);
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
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_AbstractOffHeapMap_natGetView
  (JNIEnv *env, jobject me, jlong cMap) {
    struct map *mapdata = (struct map *) cMap;
    return (jlong)mapdata->committedView;
}

static inline int computeHash(jlong arg) {
    arg *= 33;
    return (int) (arg ^ (arg >> 32));
}

static inline int computeKeyHash(jlong arg, int size) {
    arg *= 33;
    return ((int) (arg ^ (arg >> 32)) & 0x7fffffff) % size;
}

static inline int computeSlot(const struct map * const mapdata, const struct dataEntry * const e) {
    return ((mapdata->modes & IS_INDEX ? e->compressedSize : computeHash(e->key)) & 0x7fffffff) % mapdata->hashTableSize;
}

// clear all entries
static void clear(struct dataEntry **keyHash, int numEntries) {
    int i;
    for (i = 0; i < numEntries; ++i) {
        struct dataEntry *p = keyHash[i];
        while (p) {
            register struct dataEntry *next = p->nextSameHash;
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
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_AbstractOffHeapMap_natClose
    (JNIEnv *env, jobject me, jlong cMap) {
    // Get the int given the Field ID
    struct map *mapdata = (struct map *) cMap;
    clear(mapdata->keyHash, mapdata->hashTableSize);
    if (mapdata->sharedIndexLookupBuffer)
        free(mapdata->sharedIndexLookupBuffer);
    free(mapdata->keyHash);
    free(mapdata);
}


/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natGetSize
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_AbstractOffHeapMap_natGetSize
    (JNIEnv *env, jobject me, jlong cMap) {
    return ((struct map *)cMap)->count;
}


/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natClear
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_AbstractOffHeapMap_natClear
    (JNIEnv *env, jobject me, jlong cMap, jlong ctxAsLong) {
    // Get the int given the Field ID
    struct map *mapdata = (struct map *) cMap;
    struct tx_log_hdr *ctx = (struct tx_log_hdr *)ctxAsLong;
    if (!IS_TRANSACTIONAL(ctx, mapdata)) {
        // this is faster
        clear(mapdata->keyHash, mapdata->hashTableSize);
    } else {
        int hash;
        for (hash = 0; hash < mapdata->hashTableSize; ++hash) {
            struct dataEntry *e;
            for (e = mapdata->keyHash[hash]; e; e = e->nextSameHash) {
#ifdef DEBUG
                fprintf(stderr, "Transactional clear of entry %16p in hash slot %d\n", e, hash);
#endif
                record_change(env, ctx, mapdata, e, NULL);  // may throw an error....
            }
        }
    }
    memset(mapdata->keyHash, 0, mapdata->hashTableSize * sizeof(struct dataEntry *));     // set the initial pointers to NULL
    mapdata->count = 0;
}

static jbyteArray toJavaByteArray(JNIEnv *env, struct dataEntry *e) {
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


static struct dataEntry *find_entry(struct map *mapdata, jlong key) {
    int hash = computeKeyHash(key, mapdata->hashTableSize);
    struct dataEntry *e = mapdata->keyHash[hash];
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
    (JNIEnv *env, jclass me, jlong cMap, jlong key) {
    struct map *mapdata = (struct map *) cMap;
    struct dataEntry *e = find_entry(mapdata, key);
    return toJavaByteArray(env, e);
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natGetAsByteBuffer
 * Signature: (JJ)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_natGetAsByteBuffer
(JNIEnv *env, jclass me, jlong cMap, jlong key) {
    struct map *mapdata = (struct map *) cMap;
    struct dataEntry *e = find_entry(mapdata, key);
    if (!e)
        return NULL;
    if (!e->compressedSize)
        return (*env)->NewDirectByteBuffer(env, e->data, (jlong)e->uncompressedSize);
    // TODO: How to signal to the ByteBuffer that this block should be released?
    // should perform an on-heap allocation here, or set this one as "writable" to indicate it.
    char *tmp = malloc(e->uncompressedSize);
    if (!tmp) {
        throwOutOfMemory(env);
        return NULL;
    }
    LZ4_decompress_fast(e->data, tmp, e->uncompressedSize);
    return (*env)->NewDirectByteBuffer(env, tmp, (jlong)e->uncompressedSize);
//    free(tmp);
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView
 * Method:    natLength
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMapView_natLength
    (JNIEnv *env, jobject me, jlong cMap, jlong key) {
    struct map *mapdata = (struct map *) cMap;
    struct dataEntry *e = find_entry(mapdata, key);
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
    struct dataEntry *e = find_entry(mapdata, key);
    return (jint)(e ? e->compressedSize : -1);
}




// remove an entry for a key. If transactions are active, redo log / rollback info will be stored. Else the entry no longer required will be freed.
// JNIEnv may be NULL if ctx is NULL
static jboolean execRemove4Rollback(struct map * const mapdata, struct dataEntry * const oldEntry) {
    int slot = computeSlot(mapdata, oldEntry);
    struct dataEntry *prev = NULL;
    jlong key = oldEntry->key;
    for (struct dataEntry *e = mapdata->keyHash[slot]; e; e = e->nextSameHash) {
        // check if this is a match
        if (e->key == key) {
            if (!prev) {
                // initial entry, update mapdata
                mapdata->keyHash[slot] = e->nextSameHash;
            } else {
                prev->nextSameHash = e->nextSameHash;
            }
#ifdef DEBUG
            fprintf(stderr, "Removing an entry of key %ld in slot %d\n", (long)key, hash);
#endif
            free(e);
            --mapdata->count;
            return JNI_TRUE;
        }
        prev = e;
    }
    // not found. No change of size  ERROR!
    fprintf(stderr, "ERROR: Cannot remove an entry of key %ld in slot %d (does not exist)\n", (long)key, slot);
    return JNI_FALSE;
}

// COMMIT subroutine: remove an entry for a key
// only called from commitToView. Used for data map as well as index
static jboolean execRemoveShadow(struct map * const mapdata, const struct dataEntry * const ref) {
    int slot = computeSlot(mapdata, ref);
    struct dataEntry *prev = NULL;
    struct dataEntry *e = mapdata->keyHash[slot];
    jlong key = ref->key;
    while (e) {
        // check if this is a match
        if (e->key == key) {
            if (!prev) {
                // initial entry, update mapdata
                mapdata->keyHash[slot] = e->nextInCommittedView;
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


// remove an entry for a key. If transactions are active, redo log / rollback info will be stored. Else the entry no longer required will be freed.
// JNIEnv may be NULL if ctx is NULL
    struct map *mapdata = (struct map *)cMap;
    int hash = computeKeyHash(key, mapdata->hashTableSize);
    struct dataEntry *prev = NULL;
    struct dataEntry *e = mapdata->keyHash[hash];
    while (e) {
        // check if this is a match
        if (e->key == key) {
            if (!prev) {
                // initial entry, update mapdata
                mapdata->keyHash[hash] = e->nextSameHash;
            } else {
                prev->nextSameHash = e->nextSameHash;
            }
#ifdef DEBUG
            fprintf(stderr, "Removing an entry of key %ld in slot %d\n", (long)key, hash);
#endif
            record_change(env, (struct tx_log_hdr *)ctx, mapdata, e, NULL); // may throw an error
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


/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natRemove
 * Signature: (JJJ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natRemove
    (JNIEnv *env, jobject me, jlong cMap, jlong ctx, jlong key) {
    struct map *mapdata = (struct map *) cMap;
    int hash = computeKeyHash(key, mapdata->hashTableSize);
    struct dataEntry *prev = NULL;
    struct dataEntry *e = mapdata->keyHash[hash];
    while (e) {
        // check if this is a match
        if (e->key == key) {
            if (!prev) {
                // initial entry, update mapdata
                mapdata->keyHash[hash] = e->nextSameHash;
            } else {
                prev->nextSameHash = e->nextSameHash;
            }
            jbyteArray result = toJavaByteArray(env, e);
            record_change(env, (struct tx_log_hdr *)ctx, mapdata, e, NULL); // may throw an error
            --mapdata->count;
            return result;
        }
        prev = e;
        e = e->nextSameHash;
    }
    return (jbyteArray)0;
}


static struct dataEntry *create_new_entry(JNIEnv *env, jlong key, jbyteArray data, jint offset, jint length, jboolean doCompress) {
    // int uncompressed_length = (*env)->GetArrayLength(env, data);
    struct dataEntry *e;
    if (doCompress) {
        // compress the data. Allocate a target buffer
        int max_compressed_length = ROUND_UP_SIZE(LZ4_compressBound(length));
        struct dataEntry *tmp_dst = malloc(sizeof(struct dataEntry) + max_compressed_length);
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
            e = realloc(tmp_dst, sizeof(struct dataEntry) + actual_rounded_length);
            if (!e)
                e = tmp_dst;
        } else {
            e = tmp_dst;
        }
        e->compressedSize = actual_compressed_length;
    } else {
        e = (struct dataEntry *)malloc(sizeof(struct dataEntry) + ROUND_UP_SIZE(length));
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

static struct dataEntry *create_new_index_entry(JNIEnv *env, jlong key, jint hash, jbyteArray data, jint offset, jint length) {
    // int uncompressed_length = (*env)->GetArrayLength(env, data);
    struct dataEntry *e = (struct dataEntry *)malloc(sizeof(struct dataEntry) + ROUND_UP_SIZE(length));
    if (!e)
        return NULL;  // will throw OOM

    // populate the fields in order of occurence
    e->nextSameHash = NULL;   // initialize temporarily!
    e->nextInCommittedView = NULL;
    e->uncompressedSize = length;
    e->compressedSize = hash;
    e->key = key;
    if (length > 0)
        (*env)->GetByteArrayRegion(env, data, offset, length, (jbyte *)e->data);
    return e;
}

// can work on data and index structures! (index only for rollback where the key is known)
static struct dataEntry * setPutSub(struct map * const mapdata, struct dataEntry * const newEntry) {
    int slot = computeSlot(mapdata, newEntry);
    struct dataEntry *e = mapdata->keyHash[slot];
    newEntry->nextSameHash = e;  // insert it at the start
    mapdata->keyHash[slot] = newEntry;
    struct dataEntry *prev = newEntry;

    jlong key = newEntry->key;
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

// COMMIT subroutine, only called from commitToView
static struct dataEntry * setPutSubShadow(struct map * const mapdata, struct dataEntry * const newEntry) {
    int slot = computeSlot(mapdata, newEntry);
    struct dataEntry *e = mapdata->keyHash[slot];
    newEntry->nextInCommittedView = e;  // insert it at the start
    mapdata->keyHash[slot] = newEntry;
    struct dataEntry *prev = newEntry;
    jlong key = newEntry->key;

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
    struct dataEntry *newEntry = create_new_entry(env, key, data, offset, length, doCompress);
    if (!newEntry) {
        throwOutOfMemory(env);
        return JNI_FALSE;
    }

    struct dataEntry *previousEntry = setPutSub(mapdata, newEntry);
    record_change(env, (struct tx_log_hdr *)ctx, mapdata, previousEntry, newEntry);  // may throw an error
    return (previousEntry != NULL);  // true if an entry existed before
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap
 * Method:    natPut
 * Signature: (JJJ[BIIZ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapMap_natPut
    (JNIEnv *env, jobject me, jlong cMap, jlong ctx, jlong key, jbyteArray data, jint offset, jint length, jboolean doCompress) {
    struct map *mapdata = (struct map *) cMap;
    struct dataEntry *newEntry = create_new_entry(env, key, data, offset, length, doCompress);
    if (!newEntry) {
        throwOutOfMemory(env);
        return NULL;
    }

    struct dataEntry *previousEntry = setPutSub(mapdata, newEntry);
    jbyteArray result = toJavaByteArray(env, previousEntry);
    record_change(env, (struct tx_log_hdr *)ctx, mapdata, previousEntry, newEntry);  // may throw an error
    return result;
}


static int computeChainLength(struct dataEntry *e) {
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
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_AbstractOffHeapMap_natGetHistogram
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
        int len = computeChainLength(mapdata->keyHash[i]);
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
    struct dataEntry *e = find_entry(mapdata, key);
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
    struct dataEntry *e = find_entry(mapdata, key);
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
    struct dataEntry *e = find_entry(mapdata, key);
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
static int transferWrite(const int fd, char *buffer, int oldOffset, void const *src, int len) {
    while (len > 0) {
        if (oldOffset + len < FILEDUMP_BUFFER_SIZE) {
            // copy all and, still space left
            memcpy(buffer + oldOffset, src, len);
            // possibly align by multipes of 8
            if (len & 7) {
                char *p = buffer+oldOffset+len;
                do {
                    *p++ = (char)0xee;
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

static char *allocateBuffers(JNIEnv *env, char **buffer, jbyteArray filename) {

#ifdef _ISOC11_SOURCE
    *buffer = aligned_alloc(4 * 1024, FILEDUMP_BUFFER_SIZE);  // needs _ISOC11_SOURCE
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
        free(*buffer);
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
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_AbstractOffHeapMap_natWriteToFile
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
        struct dataEntry *e;
        for (e = mapdata->keyHash[i]; e; e = (fromCommittedView ? e->nextInCommittedView : e->nextSameHash)) {
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
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_AbstractOffHeapMap_natReadFromFile
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
        struct dataEntry entryHdr;
        if (fread(&(entryHdr.uncompressedSize), ENTRY_HDR_SIZE, 1, fp) != 1) {
            free(buffer);
            fclose(fp);
            throwAny(env, "Cannot read entry header");
            return;
        }
        int actualSize = entryHdr.compressedSize ? entryHdr.compressedSize : entryHdr.uncompressedSize;
        struct dataEntry *e = malloc(sizeof(struct dataEntry) + ROUND_UP_SIZE(actualSize));
        if (!e) {
            free(buffer);
            fclose(fp);
            throwOutOfMemory(env);
            return;
        }
        e->key = entryHdr.key;
        e->uncompressedSize = entryHdr.uncompressedSize;
        e->compressedSize = entryHdr.compressedSize;

        int hash = computeKeyHash(entryHdr.key, mapdata->hashTableSize);
        e->nextInCommittedView = e->nextSameHash = mapdata->keyHash[hash];
        mapdata->keyHash[hash] = e;
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
        memcpy(viewdata->keyHash, mapdata->keyHash, sizeof(struct dataEntry *) * mapdata->hashTableSize);
        viewdata->count = mapdata->count;
        viewdata->lastCommittedRef = mapdata->lastCommittedRef;
    }
}



// class member functions....

void commitToView(struct tx_log_entry *ep, jlong transactionReference) {
    ep->affected_table->lastCommittedRef = transactionReference;
    struct map *view = ep->affected_table->committedView;
    if (!view) {
        // no shadow: simple rule: discard old entry.
        struct dataEntry *e = ep->old_entry;
        if (e)
            free(e);
    } else {
        // have secondary view. Do not discard old entry, because we either still need it, or we discard it within a recursive call
        // we have a view, and are asked to replay the tx on it
        if (!ep->new_entry) {
            // was a remove => remove it on the view (which frees the old data)
            execRemoveShadow(view, ep->old_entry);
        } else {
            // insert or replace
            struct dataEntry * const shouldBeOld = setPutSubShadow(view, ep->new_entry);
            if (shouldBeOld != ep->old_entry)
                fprintf(stderr, "REDO PROBLEM: expected to get %16p, but got %16p for key %ld\n", ep->old_entry, shouldBeOld, ep->new_entry->key);
            // if new_entry was not null, then free it (it is no longer required)
            if (ep->old_entry)
                free(ep->old_entry);
        }
        view->lastCommittedRef = transactionReference;
    }
}

void print(struct tx_log_entry *ep, int i) {
    jlong key = ep->new_entry ? ep->new_entry->key : ep->old_entry->key;
    fprintf(stderr, "redo log entry %5d is for key %16ld: old=%16p, new=%16p\n", i, key, ep->old_entry, ep->new_entry);
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
        execRemove4Rollback(e->affected_table, e->new_entry);
            // TODO: if this returned flase, throw a consistency error!
        // as old_entry was null, new_entry is definitely not null and must be deallocated
        // free(e->new_entry);  // but this was done within execRenove already!
    } else {
        // replace or delete
#ifdef DEBUG
        fprintf(stderr, "    => insert / update %16p with key %ld\n", e->old_entry, e->old_entry->key);
#endif
        struct dataEntry *shouldBeNew = setPutSub(e->affected_table, e->old_entry);
        if (shouldBeNew != e->new_entry)
            fprintf(stderr, "ROLLBACK PROBLEM: expected to get %16p, but got %16p for key %ld\n", e->new_entry, shouldBeNew, e->old_entry->key);
        // if new_entry was not null, then free it (it is no longer required)
        if (e->new_entry)
            free(e->new_entry);
    }
}


// find some existing entry or return null
static struct dataEntry *findIndexEntry(struct dataEntry *e, int len, int newHash, const void *data) {
    while (e) {
        // check if this is a match (isSameIndex(e, newEntry)
        if (e->compressedSize == newHash && len == e->uncompressedSize) {
            if (len == 0 || !memcmp(e->data, data, len)) {
                // match found with same index, problem!
                return e;
            }
        }
        e = e->nextSameHash;
    }
    return NULL;
}

static struct dataEntry * setPutSubIndex(struct map * const mapdata, struct dataEntry * const newEntry) {
    int slot = (newEntry->compressedSize & 0x7fffffff) % mapdata->hashTableSize;
    struct dataEntry *existing = mapdata->keyHash[slot];
    newEntry->nextSameHash = existing;  // insert it at the start
    mapdata->keyHash[slot] = newEntry;

    if (mapdata->modes & IS_UNIQUE_UNDEX) {
//        fprintf(stderr, "try find existing: modes = %02x, hash size = %d, using slot %d\n", mapdata->modes, mapdata->hashTableSize, slot);
        // check for existing index of same value
        existing = findIndexEntry(existing, newEntry->uncompressedSize, newEntry->compressedSize, newEntry->data);
        if (existing)
            return existing;
    }
    // this is a new entry
#ifdef DEBUG
    fprintf(stderr, "Inserting an index entry of key %ld in slot %d\n", (long)key, slot);
#endif
    ++mapdata->count;
    return NULL;
}

// return previous entry or NULL for error
// old slot and new slot could be different or the same!
static struct dataEntry * setPutSubIndexReplace(struct map *mapdata, int oldHash, struct dataEntry *newEntry) {
    jlong key = newEntry->key;
    int newSlot = (newEntry->compressedSize & 0x7fffffff) % mapdata->hashTableSize;
    int oldSlot = (oldHash  & 0x7fffffff) % mapdata->hashTableSize;
    struct dataEntry *e = mapdata->keyHash[newSlot];  // the new start of chain...
    struct dataEntry *f = mapdata->keyHash[oldSlot];  // the old start of chain..., to find the previous entry for key

    newEntry->nextSameHash = e;  // insert it at the start
    mapdata->keyHash[newSlot] = newEntry;

    if (mapdata->modes & IS_UNIQUE_UNDEX) {
        // check for existing index of same value. By definition (shortcut in Java), this cannot be identical with the same key entry, we would have skipped this update!
        // check for existing index of same value
        struct dataEntry *existing = findIndexEntry(e, newEntry->uncompressedSize, newEntry->compressedSize, newEntry->data);
        if (existing)
            return NULL;
    }

    // now find the old entry to remove!
    struct dataEntry *prev = newSlot == oldSlot ? newEntry : NULL;

    while (f) {
        if (f->key == key) {
            // this one to replace
            // last plausi...
            if (f->compressedSize != oldHash)
                return NULL;        // inconsistency!
            // f is to be removed. set the ptr on prev
            if (prev)
                prev->nextSameHash = f->nextSameHash;
            else
                mapdata->keyHash[oldSlot] = f->nextSameHash;
            return f;
        }
        prev = f;
        f = f->nextSameHash;
    }
    // problem! no old entry found
    return NULL;
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndex
 * Method:    natIndexCreate
 * Signature: (JJJI[B)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndex_natIndexCreate
  (JNIEnv *env, jclass me, jlong cMap, jlong ctx, jlong key, jint hash, jbyteArray data, jint offset, jint length) {
    struct map *mapdata = (struct map *) cMap;
    struct dataEntry *newEntry = create_new_index_entry(env, key, hash, data, offset, length);
    if (!newEntry) {
        throwOutOfMemory(env);
        return;
    }

    if (setPutSubIndex(mapdata, newEntry)) {
        throwDuplicateKey(env);
        return;
    }
    record_change(env, (struct tx_log_hdr *)ctx, mapdata, 0, newEntry);  // may throw an error
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndex
 * Method:    natIndexDelete
 * Signature: (JJJI[B)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndex_natIndexDelete
  (JNIEnv *env, jclass me, jlong cMap, jlong ctx, jlong key, jint hash) {
    struct map *mapdata = (struct map *) cMap;
    int slot = (hash  & 0x7fffffff) % mapdata->hashTableSize;

    struct dataEntry *prev = NULL;
    struct dataEntry *e = mapdata->keyHash[slot];
    while (e) {
        // check if this is a match
        if (e->key == key) {
            // plausi check
            if (e->compressedSize != hash) {
                throwInconsistent(env, "hash code differs");
                return;
            }
            if (!prev) {
                // initial entry, update mapdata
                mapdata->keyHash[slot] = e->nextSameHash;
            } else {
                prev->nextSameHash = e->nextSameHash;
            }
            record_change(env, (struct tx_log_hdr *)ctx, mapdata, e, NULL); // may throw an error
            --mapdata->count;
            return;
        }
        prev = e;
        e = e->nextSameHash;
    }
    throwInconsistent(env, "no index entry found");
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndex
 * Method:    natIndexUpdate
 * Signature: (JJJII[B)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndex_natIndexUpdate
  (JNIEnv *env, jclass me, jlong cMap, jlong ctx, jlong key, jint oldHash, jint newHash, jbyteArray newData, jint offset, jint length) {
    struct map *mapdata = (struct map *) cMap;
    struct dataEntry *newEntry = create_new_index_entry(env, key, newHash, newData, offset, length);
    if (!newEntry) {
        throwOutOfMemory(env);
        return;
    }

    struct dataEntry *previousEntry = setPutSubIndexReplace(mapdata, oldHash, newEntry);
    if (!previousEntry) {
        // could have 2 causes... check uniqueness and assume it's that one!
        if (mapdata->modes & IS_UNIQUE_UNDEX)
            throwDuplicateKey(env);
        else
            throwInconsistent(env, "old index entry not found");
        return;
    }
    record_change(env, (struct tx_log_hdr *)ctx, mapdata, previousEntry, newEntry);  // may throw an error
}


// index read


static void *getTempBuffer(struct map *mapdata, int requiredSize) {
    if (mapdata->sharedIndexLookupBufferSize >= requiredSize)
        // current buffer is sufficient
        return mapdata->sharedIndexLookupBuffer;
    int newLen = ROUND_UP_SIZE(requiredSize);
    // no buffer exists so far, must malloc!
    void *buffer = malloc(newLen);
    if (!buffer)
        return buffer;      // out of memory!
    if (mapdata->sharedIndexLookupBuffer) {
        // there is a buffer already but we got a bigger one now. Free the old one and store the new.
        free(mapdata->sharedIndexLookupBuffer);
    } else  if (!(mapdata->modes & TRANSACTIONAL)) {
        // no buffer exists, but we are running possibly in multithreading mode. No buffer sharing allowed!
        return buffer;
    }
    // transactional mode, this is single threaded, no interrupt will mess up this one. Store the new buffer for reuse!
    mapdata->sharedIndexLookupBuffer = buffer;
    mapdata->sharedIndexLookupBufferSize = newLen;
    return buffer;
}

static void inline freeTempBuffer(struct map *mapdata, void *buffer) {
    if (!(mapdata->modes & TRANSACTIONAL))
        free(buffer);
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView
 * Method:    natIndexGetKey
* Signature: (JI[B)J
  */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView_natIndexGetKey
  (JNIEnv *env, jclass me, jlong cMap, jint hash, jbyteArray data, jint offset, jint length) {
    struct map *mapdata = (struct map *) cMap;
    int slot = (hash  & 0x7fffffff) % mapdata->hashTableSize;
    struct dataEntry *e = mapdata->keyHash[slot];
    if (!e) {
        // no entry at all for this hash, don't worry copying byte arrays...
        return NO_ENTRY_PRESENT;
    }

    struct dataEntry *existing;
    if (length > 0) {
        void *dataCopy = getTempBuffer(mapdata, length);
        if (!dataCopy) {
            throwOutOfMemory(env);
            return NO_ENTRY_PRESENT;
        }
        (*env)->GetByteArrayRegion(env, data, offset, length, (jbyte *)dataCopy);
        existing = findIndexEntry(e, length, hash, dataCopy);
        freeTempBuffer(mapdata, dataCopy);
    } else {
        existing = findIndexEntry(e, length, hash, NULL);
    }
    return existing ? existing->key : NO_ENTRY_PRESENT;
}

// Index Iterator

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView
 * Method:    natInit
 * Signature: (Ljava/lang/Class;)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView_natInit
  (JNIEnv *env, jclass myClass, jclass iteratorClass, jclass batchIteratorClass) {

    // Get the Field ID of the instance variables "number"
    javaIndexIteratorCurrentKeyFID = (*env)->GetFieldID(env, iteratorClass, "currentKey", "J");
    if (!javaIndexIteratorCurrentKeyFID) {
        throwAny(env, "Iterator of invoking class must have a field long currentKey");
        return;
    }
    javaIndexIteratorCurrentSizeFID = (*env)->GetFieldID(env, batchIteratorClass, "numValidEntries", "I");
    if (!javaIndexIteratorCurrentSizeFID) {
        throwAny(env, "Batched iterator of invoking class must have a field int numValidEntries");
        return;
    }
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView_PrimitiveLongKeyOffHeapMapEntryIterator
 * Method:    natIterateStart
 * Signature: (JI[B)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView_00024PrimitiveLongKeyOffHeapViewIterator_natIterateStart
  (JNIEnv *env, jobject myClass, jlong cMap, jint hash, jbyteArray data, jint length) {
    struct map *mapdata = (struct map *) cMap;
    int slot = (hash & 0x7fffffff) % mapdata->hashTableSize;
#ifdef DEBUG
    fprintf(stderr, "iterate on map index %16p (has %d entries in %d slots)\n", mapdata, mapdata->count, mapdata->hashTableSize);
#endif
    struct dataEntry *e = mapdata->keyHash[slot];
    if (!e) {
        // no entry at all for this hash, don't worry copying byte arrays...
        return (jlong)0;        // not NO_ENTRY_PRESENT, different context here!
    }

    if (length > 0) {
        void *dataCopy = getTempBuffer(mapdata, length);
        if (!dataCopy) {
            throwOutOfMemory(env);
            return NO_ENTRY_PRESENT;
        }
        (*env)->GetByteArrayRegion(env, data, 0, length, (jbyte *)dataCopy);
        e = findIndexEntry(e, length, hash, dataCopy);
        freeTempBuffer(mapdata, dataCopy);
    } else {
        e = findIndexEntry(e, length, hash, NULL);
    }
    if (e)
        (*env)->SetLongField(env, myClass, javaIndexIteratorCurrentKeyFID, e->key);
    return (jlong)e;
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView_PrimitiveLongKeyOffHeapMapEntryIterator
 * Method:    natIterate
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView_00024PrimitiveLongKeyOffHeapViewIterator_natIterate
  (JNIEnv *env, jobject myClass, jlong nextEntryPtr) {

    // this method is never called with nextEntryPtr == null
    struct dataEntry *old = (struct dataEntry *)nextEntryPtr;
    int len = old->uncompressedSize;
    for (struct dataEntry *e = old->nextSameHash; e; e = e->nextSameHash) {
        if (e->compressedSize == old->compressedSize && e->uncompressedSize == len) {
            if (len == 0 || !memcmp(e->data, old->data, len)) {
#ifdef DEBUG
                fprintf(stderr, "Found another index entry with key %ld\n", (long)(e->key));
#endif
                (*env)->SetLongField(env, myClass, javaIndexIteratorCurrentKeyFID, e->key);
                return (jlong)e;
            }
        }
    }
    // no further entry found. must be at end
    return (jlong)0;        // not NO_ENTRY_PRESENT, different context here!
}





// find some existing entry or return null
static struct dataEntry *findIndexEntries(
        JNIEnv *env, jobject myClass,
        struct dataEntry *e, int len, int newHash, const void *data,
        jlongArray dest, jint batchSize, jint recordsToSkip) {
    int found = 0;
    jlong tmp[batchSize];
    while (e && found < batchSize) {
        // check if this is a match (isSameIndex(e, newEntry)
        if (e->compressedSize == newHash && len == e->uncompressedSize) {
            if (len == 0 || !memcmp(e->data, data, len)) {
                // match found with same index
                if (recordsToSkip > 0) {
                    --recordsToSkip;
                } else {
                    tmp[found++] = e->key;
                    if (found == batchSize)
                        break;      // do not advance, as e must stay on some matching entry (index data is retrieved from here)
                }
            }
        }
        e = e->nextSameHash;
    }
    if (found > 0) {
        // fill byte array, fill number of entries found
        (*env)->SetIntField(env, myClass, javaIndexIteratorCurrentSizeFID, found);
        (*env)->SetLongArrayRegion(env, dest, 0, found, tmp);
    }
    return e;
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView_BatchedPrimitiveLongKeyOffHeapViewIterator
 * Method:    natIterateStart
 * Signature: (JI[BI[JII)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView_00024BatchedPrimitiveLongKeyOffHeapViewIterator_natIterateStart
  (JNIEnv *env, jobject myClass, jlong cMap, jint hash, jbyteArray data, jint length, jlongArray dest, jint batchSize, jint recordsToSkip) {
  struct map *mapdata = (struct map *) cMap;
  int slot = (hash & 0x7fffffff) % mapdata->hashTableSize;
  struct dataEntry *e = mapdata->keyHash[slot];
  if (!e) {
      // no entry at all for this hash, don't worry copying byte arrays...
      return (jlong)0;        // not NO_ENTRY_PRESENT, different context here!
  }

  if (length > 0) {
      void *dataCopy = getTempBuffer(mapdata, length);
      if (!dataCopy) {
          throwOutOfMemory(env);
          return NO_ENTRY_PRESENT;
      }
      (*env)->GetByteArrayRegion(env, data, 0, length, (jbyte *)dataCopy);
      e = findIndexEntries(env, myClass, e, length, hash, dataCopy, dest, batchSize, recordsToSkip);
      freeTempBuffer(mapdata, dataCopy);
  } else {
      e = findIndexEntries(env, myClass, e, length, hash, NULL, dest, batchSize, recordsToSkip);
  }
  return (jlong)e;
}

/*
 * Class:     de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView_BatchedPrimitiveLongKeyOffHeapViewIterator
 * Method:    natIterate
 * Signature: (J[JI)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_PrimitiveLongKeyOffHeapIndexView_00024BatchedPrimitiveLongKeyOffHeapViewIterator_natIterate
  (JNIEnv *env, jobject myClass, jlong nextEntryPtr, jlongArray dest, jint batchSize) {
    // this method is never called with nextEntryPtr == null
    int found = 0;
    jlong tmp[batchSize];
    struct dataEntry *e = (struct dataEntry *)nextEntryPtr;
    struct dataEntry *old = e;
    int len = old->uncompressedSize;
    int hash = old->compressedSize;
    for (e = e->nextSameHash; e; e = e->nextSameHash) {
        // check if this is a match (isSameIndex(e, newEntry)
        if (len == e->uncompressedSize && e->compressedSize == hash) {
            if (len == 0 || !memcmp(e->data, old->data, len)) {
                // match found with same index
                tmp[found++] = e->key;
                if (found == batchSize)
                    break;
            }
        }
    }
    if (found > 0) {
        // fill byte array, fill number of entries found
        (*env)->SetIntField(env, myClass, javaIndexIteratorCurrentSizeFID, found);
        (*env)->SetLongArrayRegion(env, dest, 0, found, tmp);
    }
    return (jlong)e;
}



/*
 * Class:     de_jpaw_offHeap_AbstractOffHeapMap
 * Method:    natFullDump
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_AbstractOffHeapMap_natFullDump
  (JNIEnv *env, jclass me, jlong cMap) {
    struct map *mapdata = (struct map *) cMap;

    printf("Map is at %16p, hash size %d, %d entries, modes=%02x\n", mapdata, mapdata->hashTableSize, mapdata->count, mapdata->modes);
    for (int i = 0; i < mapdata->hashTableSize; ++i) {
        if (mapdata->keyHash[i]) {
            printf("Slot %d:\n", i);
            for (struct dataEntry *e = mapdata->keyHash[i]; e; e = e->nextSameHash) {
                printf("    key %08lx: len=%9d hash=%08x\n", e->key, e->uncompressedSize, e->compressedSize);
            }
        }
    }
}
