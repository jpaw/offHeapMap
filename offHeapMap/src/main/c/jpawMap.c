#undef __cplusplus
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include "jpawMap.h"

struct entry {
	int uncompressedSize;
	int compressedSize;  // 0 = is not compressed
	jlong key;
	char data[];
};

struct map {
	int currentEntries;
	int maxEntries;
	int compressAboveSize;
	int *collisionIndicator;
	struct entry **data;
};

static jclass thisClass;
static jfieldID fidNumber;

// reference: see http://www3.ntu.edu.sg/home/ehchua/programming/java/JavaNativeInterface.html

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    natMake
 * Signature: (II)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_natMake
  (JNIEnv *env, jobject me, jint size, jint compressAboveSize) {
	// round up the size to multiples of 32, for the collision indicator
	size = ((size - 1) | 31) + 1;
	struct map *mapdata = malloc(sizeof(struct map));
	mapdata->currentEntries = 0;
	mapdata->maxEntries = size;
	mapdata->compressAboveSize = compressAboveSize;
	mapdata->collisionIndicator = calloc(size >> 5, sizeof(int));
	mapdata->data = calloc(size, sizeof(struct entry *));

	thisClass = (*env)->NewGlobalRef(env, (*env)->GetObjectClass(env, me));  // call to newGlobalRef is required because otherwise jclass is only valid for the current call

    // Get the Field ID of the instance variables "number"
    fidNumber = (*env)->GetFieldID(env, thisClass, "cStruct", "J");
    if (NULL == fidNumber)
    	return;

    printf("jpawMap: created new map at %p\n", mapdata);
    (*env)->SetLongField(env, me, fidNumber, (jlong)mapdata);
}

int computeHash(jlong arg, int size) {
    arg *= 33;
    return ((int)(arg ^ (arg >> 32)) & 0x7fffffff) % size;
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    close
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_close
  (JNIEnv *env, jobject me) {
    // Get the int given the Field ID
	struct map *mapdata = (struct map *)((*env)->GetLongField(env, me, fidNumber));
    printf("jpawMap: closing map at %p\n", mapdata);
    (*env)->SetLongField(env, me, fidNumber, (jlong)0);
}

static jbyteArray toJavaByteArray(JNIEnv *env, struct entry *e) {
	if (!e)
		return (jbyteArray)0;
	jbyteArray result = (*env)->NewByteArray(env, e->uncompressedSize);
	(*env)->SetByteArrayRegion(env, result, 0, e->uncompressedSize, e->data);
	return result;
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    get
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_get
  (JNIEnv *env, jobject me, jlong key) {
	struct map *mapdata = (struct map *)((*env)->GetLongField(env, me, fidNumber));
	int hash = computeHash(key, mapdata->maxEntries);
	return toJavaByteArray(env, mapdata->data[hash]);
}

struct entry * setPutSub(JNIEnv *env, jobject me, jlong key, jbyteArray data) {
	struct entry *previousEntry = NULL;
	struct map *mapdata = (struct map *)((*env)->GetLongField(env, me, fidNumber));
	int hash = computeHash(key, mapdata->maxEntries);
	struct entry *e = mapdata->data[hash];

	if (e != NULL) {
		// release old data
		--mapdata->currentEntries;
		previousEntry = e;
	}
	if (data == NULL) {
		mapdata->data[hash] = NULL;
	} else {
		// TODO: optimize for the case where old / new size are similar / same bucket
		++mapdata->currentEntries;
		jsize newLength = (*env)->GetArrayLength(env, data);
		struct entry *newEntry = malloc(sizeof(struct entry) + newLength);
		newEntry->uncompressedSize = newLength;
		newEntry->compressedSize = 0;
		newEntry->key = key;
		(*env)->GetByteArrayRegion(env, data, 0, newLength, newEntry->data);
		mapdata->data[hash] = newEntry;
	}
	return previousEntry;
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    set
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_set
  (JNIEnv *env, jobject me, jlong key, jbyteArray data) {
	struct entry *previousEntry = setPutSub(env, me, key, data);

	if (previousEntry != NULL)
		free(previousEntry);
}

/*
 * Class:     de_jpaw_offHeap_LongToByteArrayOffHeapMap
 * Method:    put
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_LongToByteArrayOffHeapMap_put
  (JNIEnv *env, jobject me, jlong key, jbyteArray data) {
	struct entry *previousEntry = setPutSub(env, me, key, data);

	if (previousEntry != NULL) {
		jbyteArray result = toJavaByteArray(env, previousEntry);
		free(previousEntry);
		return result;
	}
	return NULL;
}
