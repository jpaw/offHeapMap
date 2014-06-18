/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap */

#ifndef _Included_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
#define _Included_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natInit
 * Signature: (Ljava/lang/Class;)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natInit
  (JNIEnv *, jclass, jclass);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natOpen
 * Signature: (IIZ)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natOpen
  (JNIEnv *, jobject, jint, jint, jboolean);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natGetView
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natGetView
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natClose
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natClose
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natClear
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natClear
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natWriteToFile
 * Signature: (J[BZ)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natWriteToFile
  (JNIEnv *, jobject, jlong, jbyteArray, jboolean);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natReadFromFile
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natReadFromFile
  (JNIEnv *, jobject, jlong, jbyteArray);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natDelete
 * Signature: (JJJ)Z
 */
JNIEXPORT jboolean JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natDelete
  (JNIEnv *, jobject, jlong, jlong, jlong);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natGetSize
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natGetSize
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natLength
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natLength
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natCompressedLength
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natCompressedLength
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natGet
 * Signature: (JJ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natGet
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natRemove
 * Signature: (JJJ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natRemove
  (JNIEnv *, jobject, jlong, jlong, jlong);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natSet
 * Signature: (JJJ[BIIZ)Z
 */
JNIEXPORT jboolean JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natSet
  (JNIEnv *, jobject, jlong, jlong, jlong, jbyteArray, jint, jint, jboolean);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natPut
 * Signature: (JJJ[BIIZ)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natPut
  (JNIEnv *, jobject, jlong, jlong, jlong, jbyteArray, jint, jint, jboolean);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natGetHistogram
 * Signature: (J[I)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natGetHistogram
  (JNIEnv *, jobject, jlong, jintArray);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natGetIntoPreallocated
 * Signature: (JJ[BI)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natGetIntoPreallocated
  (JNIEnv *, jobject, jlong, jlong, jbyteArray, jint);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natGetRegion
 * Signature: (JJII)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natGetRegion
  (JNIEnv *, jobject, jlong, jlong, jint, jint);

/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap
 * Method:    natGetField
 * Signature: (JJIBB)[B
 */
JNIEXPORT jbyteArray JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_natGetField
  (JNIEnv *, jobject, jlong, jlong, jint, jbyte, jbyte);

#ifdef __cplusplus
}
#endif
#endif
/* Header for class de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_PrimitiveLongKeyOffHeapMapEntry */

#ifndef _Included_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_PrimitiveLongKeyOffHeapMapEntry
#define _Included_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_PrimitiveLongKeyOffHeapMapEntry
#ifdef __cplusplus
extern "C" {
#endif
#ifdef __cplusplus
}
#endif
#endif
/* Header for class de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_PrimitiveLongKeyOffHeapMapEntryIterator */

#ifndef _Included_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_PrimitiveLongKeyOffHeapMapEntryIterator
#define _Included_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_PrimitiveLongKeyOffHeapMapEntryIterator
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_PrimitiveLongKeyOffHeapMapEntryIterator
 * Method:    natIterate
 * Signature: (JJI)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_AbstractPrimitiveLongKeyOffHeapMap_00024PrimitiveLongKeyOffHeapMapEntryIterator_natIterate
  (JNIEnv *, jobject, jlong, jlong, jint);

#ifdef __cplusplus
}
#endif
#endif
/* Header for class de_jpaw_offHeap_OffHeapTransaction */

#ifndef _Included_de_jpaw_offHeap_OffHeapTransaction
#define _Included_de_jpaw_offHeap_OffHeapTransaction
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCreateTransaction
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCreateTransaction
  (JNIEnv *, jobject, jint);

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natSetMode
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natSetMode
  (JNIEnv *, jobject, jlong, jint);

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCommit
 * Signature: (JJZ)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCommit
  (JNIEnv *, jobject, jlong, jlong, jboolean);

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natRollback
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natRollback
  (JNIEnv *, jobject, jlong, jint);

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natSetSafepoint
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natSetSafepoint
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCloseTransaction
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCloseTransaction
  (JNIEnv *, jobject, jlong);

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natDebugRedoLog
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natDebugRedoLog
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif
