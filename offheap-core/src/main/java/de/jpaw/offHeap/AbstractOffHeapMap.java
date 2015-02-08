package de.jpaw.offHeap;

import java.io.PrintStream;

import de.jpaw.collections.ByteArrayConverter;
import de.jpaw.collections.OffHeapBaseMap;

/** Base class for data and index maps. The generic type T represents either the data or the index type. */
public class AbstractOffHeapMap<T> implements OffHeapBaseMap {
    
    /** Only used by native code, to store the off heap address of the structure. */
    public final ByteArrayConverter<T> converter;  // this is usually the superclass itself
    public final boolean isView;
    public final String name;
    protected final long cStruct;
    
    // class can only be instantiated from a parent
    protected AbstractOffHeapMap(ByteArrayConverter<T> converter, long cMap, boolean isView, String name) {
        this.converter = converter;
        this.cStruct = cMap;
        this.isView = isView;
        this.name = name;
    }
    
    //
    // internal native API
    /** Returns the number of entries in the JNI data structure. */
    private static native int natGetSize(long cMap);
    
    /** Returns the number of entries in the JNI data structure. */
    private static native void natFullDump(long cMap);
    
    /** Returns a histogram of the hash distribution. For each entry in the array, the number of hash chains with this length is provided.
     * Chains of bigger length are not counted. The method returns the longest chain length. */
    private static native int natGetHistogram(long cMap, int [] chainsOfLength);
    
    /** Allocates and initializes a new data structure for the given maximum number of elements.
     * Returns the resulting off heap location.
     */
    protected static native long natOpen(int size, int modes, boolean withCommittedView);
    
    /** Returns the read-only shadow (committed view) of the map, or null if this map does not have a shadow. */
    protected static native long natGetView(long cMap);
    
    /** Closes the map (clears the map and all entries it from memory).
     * After close() has been called, the object should not be used any more. */
    protected static native void natClose(long cMap);
    
    /** Deletes all entries from the map, but keeps the map structure itself. */
    protected static native void natClear(long cMap, long ctx);
    
    /** Dump the full database contents to a disk file. */
    protected static native void natWriteToFile(long cMap, byte [] pathname, boolean fromCommittedView);

    /** Read a database from disk. The database should be empty before. */
    protected static native void natReadFromFile(long cMap, byte [] pathname);
    

    @Override
    public boolean isReadonly() {
        return isView;
    }
    
    /** Returns the number of entries currently in the map. */
    @Override
    public int size() {
        return natGetSize(cStruct);
    }
    
    /** Returns a histogram of the hash distribution. For each entry in the array, the number of hash chains with this length is provided.
     * Chains of bigger length are not counted. The method returns the longest chain length. */
    public int getHistogram(int [] chainsOfLength) {
        return natGetHistogram(cStruct, chainsOfLength);
    }
    
    private static final int [] NO_ARRAY = new int [0];
    public void verifyMaxChainLength(int maxLen) {
        int actualMaxLen = natGetHistogram(cStruct, NO_ARRAY);
        if (actualMaxLen > maxLen) {
            System.err.println("Validation error: maxlen is " + actualMaxLen);
            throw new RuntimeException(actualMaxLen + " > " + maxLen);
        }
    }
    
    /** Prints the histogram of the hash distribution. */
    public void printHistogram(int len, PrintStream out) {
        if (out == null)
            out = System.out;
        int [] histogram = new int [len];
        int maxChainLen = natGetHistogram(cStruct, histogram);
        out.println("Currently " + size() + " entries are stored, with maximum chain length " + maxChainLen);
        for (int i = 0; i < len; ++i)
            out.println(String.format("%6d Chains of length %3d", histogram[i], i));
    }

    /** Prints the histogram of the hash distribution. */
    public void fullDump() {
        natFullDump(cStruct);
    }
    
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }
}
