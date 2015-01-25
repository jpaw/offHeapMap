package de.jpaw.offHeap;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import de.jpaw.collections.ByteArrayConverter;
import de.jpaw.collections.PrimitiveLongKeyMapView;

/** Implements a long -> byte [] hash map off heap, using JNI.
 * null values cannot be stored in the map (as they are used to indicate a missing entry, but zero-length byte arrays can.
 * Zero length arrays are therefore never compressed.
 * 
 * This class should be inherited in order to create specific implementations fir fixed types of V, while the native implementation is fixed to byte arrays.
 * 
 *  This implementation is not thread-safe. */
public class PrimitiveLongKeyOffHeapMapView<V> extends AbstractOffHeapMap<V> implements PrimitiveLongKeyMapView<V> {
    
    static {
        OffHeapInit.init();
        natInit(PrimitiveLongKeyOffHeapMapView.PrimitiveLongKeyOffHeapMapEntryIterator.class);
    }
    
    // class can only be instantiated from a parent
    protected PrimitiveLongKeyOffHeapMapView(ByteArrayConverter<V> converter, long cMap, boolean isView) {
        super(converter, cMap, isView);
    }
    
    //
    // internal native API
    //
  
    /** Register globals (especially the Iterator class). */
    private static native void natInit(Class<?> arg);

    /** Returns the (uncompressed) size of the data stored for key, or -1 if null / no entry is stored for key. */
    private static native int natLength(long cMap, long key);
    
    /** Returns the compressed size of a stored entry, or -1 if no entry is stored, or 0 if the data is not compressed. */
    private static native int natCompressedLength(long cMap, long key);
    
    /** Read an entry and return it in uncompressed form. Returns null if no entry is present for the specified key. */
    private static native byte [] natGet(long cMap, long key);
    
    /** Read an entry and return it as a DirectByteBuffer which is created from JNI. This avoids a buffer copy. */
    private static native ByteBuffer natGetAsByteBuffer(long cMap, long key);
    
    /** Copy an entry into a preallocated byte area, at a certain offset. */
    private static native int natGetIntoPreallocated(long cMap, long key, byte [] target, int offset);
    
    /** Return a portion of a stored element, determined by offset and length.
     * The purpose of this method is to allow the transfer of a small portion of the data.
     * returns -1 if the entry did not exist, or the number of bytes transferred. */
    private static native byte [] natGetRegion(long cMap, long key, int offset, int length);

    /** Return a portion of a stored element, determined by field delimiters, excluding the delimiters.
     * Field numbering starts with 0. The first delimiter acts as a field separator, such as comma in CSV,
     * the second is an alternate delimiter, which indicates the following field should be interpreted as null.
     * (Normally, a field is considered as null only if the data ends before.) If the second delimiter is not desired,
     * assign it the same value as the first delimiter. */
    private static native byte [] natGetField(long cMap, long key, int fieldNo, byte delimiter, byte nullIndicator);

    //
    // External API, as a wrapper to the internal native one.
    // The Java methods maintain the current size, in order to allow fast access to it from Java without the need to perform a JNI call.
    // Also, the decision when to compress an entry is done within Java for added flexibility (for example overwriting the decision method).
    //
    
    /** Removes the entry stored for key from the map (if it did exist). */
    @Override
    public void delete(long key) {
        throw new UnsupportedOperationException("Cannot delete on a readonly view");
    }
    
    /** Read an entry and return it in uncompressed form. Returns null if no entry is present for the specified key. */
    @Override
    public V get(long key) {
        return converter.byteArrayToValueType(natGet(cStruct, key));
    }
    
    /** returns the data as a DirectByteBuffer. */
    public ByteBuffer getAsByteBuffer(long key) {
        return natGetAsByteBuffer(cStruct, key);
    }
    
    /** Returns the length of a stored entry, or -1 if no entry is stored. */
    public int length(long key) {
        return natLength(cStruct, key);
    }
    
    /** Returns the compressed size of a stored entry, or -1 if no entry is stored, or 0 if the data is not compressed. */
    public int compressedLength(long key) {
        return natCompressedLength(cStruct, key);
    }
    
    /** Stores an entry in the map and returns the previous entry, or null if there was no prior entry for this key.
     * Deleting an entry can be done by passing null as the data pointer. */
    @Override
    public V put(long key, V data) {
        throw new UnsupportedOperationException("Cannot delete on a readonly view");
    }

    /** Returns true if an entry is stored for key (i.e. get(key) would return non null), and false otherwise. */
    @Override
    public boolean containsKey(long key) {
        return natLength(cStruct, key) >= 0;
    }

    @Override
    public Iterator<PrimitiveLongKeyMapView.Entry<V>> iterator() {
        return new PrimitiveLongKeyOffHeapMapEntryIterator();
    }
    
    /** Return a portion of a stored element, determined by offset and length.
     * The purpose of this method is to allow the transfer of a small portion of the data.
     * returns -1 if the entry did not exist, or the number of bytes transferred. */
    public byte [] getRegion(long key, int offset, int length) {
        return natGetRegion(cStruct, key, offset, length);
    }
    
    /** Return an object into an existing buffer. The number of bytes written is returned, or -1 if the object did not exist.
     * The number of bytes written can be smaller than the full object would require, if the target buffer is too small.
     */
    public int getIntoBuffer(long key, byte [] buffer, int offset) {
        return natGetIntoPreallocated(cStruct, key, buffer, offset);
    }
    
    /** Return a portion of a stored element, determined by field delimiters, excluding the delimiters.
     * Field numbering starts with 0. The first delimiter acts as a field separator, such as comma in CSV,
     * the second is an alternate delimiter, which indicates the following field should be interpreted as null.
     * (Normally, a field is considered as null only if the data ends before.) If the second delimiter is not desired,
     * assign it the same value as the first delimiter. */
    public V getField(long key, int fieldNo, byte delimiter, byte nullIndicator) {
        return converter.byteArrayToValueType(natGetField(cStruct, key, fieldNo, delimiter, nullIndicator));
    }
    public V getField(long key, int fieldNo, byte delimiter) {
        return getField(key, fieldNo, delimiter, delimiter);
    }
    
    
    // protected proxy for access from index class
    protected PrimitiveLongKeyOffHeapMapEntry createEntry(long key) {
        return new PrimitiveLongKeyOffHeapMapEntry(key);
    }
    
    /** The Map.Entry is just a proxy to the real data (flyweight). The value is obtained from the backing store on demand.
     * An optimized version could store a direct pointer to the off-Heap data, avoiding the hash computation, or even cache the value
     * in case multiple accesses are done. */
    public class PrimitiveLongKeyOffHeapMapEntry implements PrimitiveLongKeyMapView.Entry<V> {
        private final long key;
        
        private PrimitiveLongKeyOffHeapMapEntry(long key) {
            this.key = key;
        }

        @Override
        public long getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return get(key);
        }

        @Override
        public V setValue(V value) {
            return put(key, value);     // if we are working on a view, this may throw UnsupportedOperationException
        }
    }
    
    
    private class PrimitiveLongKeyOffHeapMapEntryIterator implements Iterator<PrimitiveLongKeyMapView.Entry<V>> {
        private PrimitiveLongKeyOffHeapMapEntry currentEntry = null; // the cached last entry
        
        private long nextEntryPtr = 0L;     // we are "at End" if this field has value 0
        private long currentKey = 0L;       // updated from JNI
        private int currentHashIndex = 0;   // updated from JNI
        
        private native long natIterate(long cStructOfMap, long previousEntryPtr, int currentHashIndex);
        
        /** Constructor, protected because it can only be created by the Map itself. */
        protected PrimitiveLongKeyOffHeapMapEntryIterator() {
            nextEntryPtr = natIterate(cStruct, 0L, -1);   // true = isInitial
        }
        
        public PrimitiveLongKeyOffHeapMapEntry getCurrent() {
            if (currentEntry == null)
                throw new NoSuchElementException();
            return currentEntry;
        }
        
        @Override
        public boolean hasNext() {
            return nextEntryPtr != 0L;
        }

        @Override
        public PrimitiveLongKeyOffHeapMapEntry next() {
            if (nextEntryPtr == 0L)
                throw new NoSuchElementException();
            currentEntry = new PrimitiveLongKeyOffHeapMapEntry(currentKey);
            nextEntryPtr = natIterate(cStruct, nextEntryPtr, currentHashIndex);   // peek to the one after this (to allow removing the returned one)
            return currentEntry;
        }
        
        @Override
        public void remove() {
            if (currentEntry == null)
                throw new NoSuchElementException();
            delete(currentEntry.getKey());
        }
    }
    
}
