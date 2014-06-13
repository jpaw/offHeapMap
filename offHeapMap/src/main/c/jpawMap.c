#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jni.h>
#include <lz4.h>
#include "jpawMap.h"

struct entry {
    int uncompressedSize;
    int compressedSize;  // 0 = is not compressed
    jlong key;
    struct entry *nextSameHash;
    char data[];
};

struct map {
    int currentEntries;
    int maxEntries;
    struct entry **data;
};

static jclass thisClass;
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

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natOpen
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natOpen(JNIEnv *env, jobject me, jint size) {
    thisClass = (*env)->NewGlobalRef(env, (*env)->GetObjectClass(env, me)); // call to newGlobalRef is required because otherwise jclass is only valid for the current call

    // Get the Field ID of the instance variables "number"
    fidNumber = (*env)->GetFieldID(env, thisClass, "cStruct", "J");
    if (!fidNumber) {
        throwAny(env, "Invoking class must have a field long cStruct");
        return;
    }

    // round up the size to multiples of 32, for the collision indicator
    size = ((size - 1) | 31) + 1;
    struct map *mapdata = malloc(sizeof(struct map));
    if (!mapdata) {
        throwOutOfMemory(env);
        return;
    }
    mapdata->currentEntries = 0;
    mapdata->maxEntries = size;
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
    clear(mapdata->data, mapdata->maxEntries);
    free(mapdata->data);
    free(mapdata);
    (*env)->SetLongField(env, me, fidNumber, (jlong) 0);
    (*env)->DeleteGlobalRef(env, thisClass);
    thisClass = NULL;
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natClear
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natClear(JNIEnv *env, jobject me) {
    // Get the int given the Field ID
    struct map *mapdata = (struct map *) ((*env)->GetLongField(env, me, fidNumber));
    clear(mapdata->data, mapdata->maxEntries);
    memset(mapdata->data, 0, mapdata->maxEntries * sizeof(struct entry *));     // set the initial pointers to NULL
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
    int hash = computeHash(key, mapdata->maxEntries);
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
    int hash = computeHash(key, mapdata->maxEntries);
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
    int hash = computeHash(key, mapdata->maxEntries);
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
    int hash = computeHash(key, mapdata->maxEntries);
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
