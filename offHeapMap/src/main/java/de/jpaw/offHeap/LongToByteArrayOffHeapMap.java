package de.jpaw.offHeap;

import java.io.PrintStream;

/** Implements a long -> byte [] hash map off heap, using JNI.
 * null values cannot be stored in the map (as they are used to indicate a missing entry, but zero-length byte arrays can.
 * Zero length arrays are therefore never compressed.
 * 
 *  This implementation is not thread-safe. */
public class LongToByteArrayOffHeapMap {
    /** Internal state to indicate if the native libraries have been loaded. It is false after the class has been initialized
     * and will be set to true once the first map should be created.
     */
    private static Boolean isInitialized = Boolean.FALSE;
    
    private final Shard myShard;
    
    /** Only used by native code, to store the off heap address of the structure. */
    private long cStruct = 0L;
    
    /** The current number of elements inside the offheap storage. Identical meaning to java.util.Map.size().
     * Only valid for non-transactional maps. */
//    private int currentSize = 0;
    
    /** The threshold at which entries stored should be automatically compressed, in bytes.
     * Setting it to Integer.MAX_VALUE will disable compression. Setting it to 0 will perform compression for all (non-zero-length) items. */
    private int maxUncompressedSize = Integer.MAX_VALUE;

    
    //
    // internal native API
    //
    
    /** Initialization of the JNI code. Must be called before any other method is used. May only be called once.
     */
    private native void natInit();
    
    /** Allocates and initializes a new data structure for the given maximum number of elements.
     * Stores the resulting off heap location in the private field cStruct.
     */
    private native void natOpen(int size, int modes);
    
    /** Closes the map (clears the map and all entries it from memory).
     * After close() has been called, the object should not be used any more. */
    private native void natClose();
    
    /** Deletes all entries from the map, but keeps the map structure itself. */
    private native void natClear();
    
    /** Removes an entry from the map. returns true if it was removed, false if no data was present. */
    private native boolean natRemove(long ctx, long key);
    
    /** Returns the number of entries in the JNI data structure. */
    private native int natGetSize();
    
    /** Returns the (uncompressed) size of the data stored for key, or -1 if null / no entry is stored for key. */
    private native int natLength(long key);
    
    /** Returns the compressed size of a stored entry, or -1 if no entry is stored, or 0 if the data is not compressed. */
    private native int natCompressedLength(long key);
    
    /** Read an entry and return it in uncompressed form. Returns null if no entry is present for the specified key. */
    private native byte [] natGet(long key);
    
    /** Read an entry and return it in uncompressed form, then deletes it from the structure.
     *  Returns null if no entry is present for the specified key. */
    private native byte [] natGetAndRemove(long ctx, long key);
    
    /** Stores an entry in the map. Returns true if this was a new entry. Returns false if the operation has overwritten existing data.
     * The new data will be compressed if indicated by the last parameter. data may not be null (use remove(key) for that purpose). */
    private native boolean natSet(long ctx, long key, byte [] data, boolean doCompress);
    
    /** Stores an entry in the map, specified by a region within some data. Returns true if this was a new entry. Returns false if the operation has overwritten existing data.
     * The new data will be compressed if indicated by the last parameter. data may not be null (use remove(key) for that purpose). */
    private native boolean natStoreRegion(long ctx, long key, byte [] data, int offset, int length, boolean doCompress);
    
    /** Stores an entry in the map and returns the previous entry, or null if there was no prior entry for this key.
     * data may not be null (use get(key) for that purpose). */
    private native byte [] natPut(long ctx, long key, byte [] data, boolean doCompress);
    
    /** Returns a histogram of the hash distribution. For each entry in the array, the number of hash chains with this length is provided.
     * Chains of bigger length are not counted. The method returns the longest chain length. */
    private native int natGetHistogram(int [] chainsOfLength);
    
    
    
    //
    // External API, as a wrapper to the internal native one.
    // The Java methods maintain the current size, in order to allow fast access to it from Java without the need to perform a JNI call.
    // Also, the decision when to compress an entry is done within Java for added flexibility (for example overwriting the decision method).
    //
    
    protected boolean shouldICompressThis(byte [] data) {
        return data.length > maxUncompressedSize;
    }
    
    public LongToByteArrayOffHeapMap(int size, Shard forShard, int modes) {
        myShard = forShard; 
        synchronized (isInitialized) {
            if (!isInitialized) {
                OffHeapTransaction.init();
                natInit();
                // now we are (unless an Exception was thrown)
                isInitialized = Boolean.TRUE;
            }
        }
        natOpen(size, modes);
    }
    public LongToByteArrayOffHeapMap(int size, Shard forShard) {
        this(size, Shard.TRANSACTIONLESS_DEFAULT_SHARD, -1);
    }
    public LongToByteArrayOffHeapMap(int size) {
        this(size, Shard.TRANSACTIONLESS_DEFAULT_SHARD, 0);
    }
    
    /** Returns the number of entries currently in the map. */
    public int size() {
        return natGetSize();
    }
    
    /** Returns a histogram of the hash distribution. For each entry in the array, the number of hash chains with this length is provided.
     * Chains of bigger length are not counted. The method returns the longest chain length. */
    public int getHistogram(int [] chainsOfLength) {
        return natGetHistogram(chainsOfLength);
    }
    
    
    public int getMaxUncompressedSize() {
        return maxUncompressedSize;
    }

    public void setMaxUncompressedSize(int maxUncompressedSize) {
        if (maxUncompressedSize < 0)
            throw new IllegalArgumentException("maxUncompressedSize may not be < 0");
        this.maxUncompressedSize = maxUncompressedSize;
    }
    
    /** Closes the map (clears the map and all entries it from memory).
     * After close() has been called, the object should not be used any more. */
    public void close() {
        natClose();
    }
    
    /** Deletes all entries from the map, but keeps the map structure itself. */
    public void clear() {
        if (myShard != Shard.TRANSACTIONLESS_DEFAULT_SHARD)
            throw new RuntimeException("clear() only possible for maps of the transactionless default shard");
        natClear();
    }

    /** Removes the entry stored for key from the map (if it did exist). */
    public void remove(long key) {
        natRemove(myShard.getTxCStruct(), key);
    }
    
    /** Read an entry and return it in uncompressed form. Returns null if no entry is present for the specified key. */
    public byte [] get(long key) {
        return natGet(key);
    }
    
    /** Returns true if an entry is stored for key (i.e. get(key) would return non null), and false otherwise. */
    public boolean test(long key) {
        return natLength(key) >= 0;
    }
    
    /** Returns the length of a stored entry, or -1 if no entry is stored. */
    public int length(long key) {
        return natLength(key);
    }
    
    /** Returns the compressed size of a stored entry, or -1 if no entry is stored, or 0 if the data is not compressed. */
    public int compressedLength(long key) {
        return natCompressedLength(key);
    }
    
    /** Stores an entry in the map.
     * The new data will be compressed if it is bigger than the threshold passed in map creation.
     * Deleting an entry can be done by passing null as the data pointer. */
    public void set(long key, byte [] data) {
        if (data == null) {
            remove(key);
        } else {
            natSet(myShard.getTxCStruct(), key, data, shouldICompressThis(data));
        }
    }
    
    /** Stores an entry in the map and returns the previous entry, or null if there was no prior entry for this key.
     * Deleting an entry can be done by passing null as the data pointer. */
    public byte [] put(long key, byte [] data) {
        if (data == null) {
            return natGetAndRemove(myShard.getTxCStruct(), key);
        } else {
            return natPut(myShard.getTxCStruct(), key, data, shouldICompressThis(data));
        }
    }
    
    
    /** Stores an entry in the map, specified by a region within a byte array.
     * The new data will be compressed if it is bigger than the threshold passed in map creation.
     * Deleting an entry can be done by passing null as the data pointer. */
    public void storeRegion(long key, byte [] data, int offset, int length) {
        if (data == null || offset < 0 || offset + length > data.length)
            throw new IllegalArgumentException();
        natStoreRegion(myShard.getTxCStruct(), key, data, offset, length, shouldICompressThis(data));
    }

    /** Prints the histogram of the hash distribution. */
    public void printHistogram(int len, PrintStream out) {
        if (out == null)
            out = System.out;
        int [] histogram = new int [len];
        int maxChainLen = natGetHistogram(histogram);
        out.println("Currently " + size() + " entries are stored, with maximum chain length " + maxChainLen);
        for (int i = 0; i < len; ++i)
            out.println(String.format("%6d Chains of length %3d", histogram[i], i));
    }
}
