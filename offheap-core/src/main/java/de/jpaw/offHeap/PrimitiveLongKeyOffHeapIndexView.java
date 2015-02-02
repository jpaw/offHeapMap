package de.jpaw.offHeap;

import java.util.Iterator;
import java.util.NoSuchElementException;

import de.jpaw.collections.ByteArrayConverter;

/** All ooperations which are possible on a committed view onto an index. */
public class PrimitiveLongKeyOffHeapIndexView<I> extends AbstractOffHeapMap<I> {
    
    static {
        OffHeapInit.init();
        natInit(PrimitiveLongKeyOffHeapIndexView.PrimitiveLongKeyOffHeapViewIterator.class);
    }
    
    // class can only be instantiated from a parent
    protected PrimitiveLongKeyOffHeapIndexView(
            long cMap,
            boolean isView,
            ByteArrayConverter<I> indexConverter) {
        super(indexConverter, cMap, isView);
    }

//    protected byte [] indexData(I index) {
//        return converter == null ? null : converter.valueTypeToByteArray(index);
//    }
    
    protected int indexHash(I index) {
        return converter == null ? (Integer)index : index.hashCode();
    }

    /** Register globals (especially the Iterator class). */
    private static native void natInit(Class<?> arg);

    /** Read an entry and return its key, or null if it does not exist. */
    private static native long natIndexGetKey(long cMap, int indexHash, byte [] indexData, int length);
    
    
    /** returns the key for an index or 0 if there is no entry for this key.
     * TODO: throws dup_val_on_index if there is no unique key. */
    public long getUniqueKeyByIndex(I index) {
        byte [] indexData = converter.getBuffer(index);
        return natIndexGetKey(cStruct, indexHash(index), indexData, converter.getLength());
    }
    
    public Long getUniqueKeyByIndexDirect(int index) {
        return natIndexGetKey(cStruct, index, null, 0);
    }
    
    private class IndexIterable implements Iterable<Long> {
        final I index;
        private IndexIterable(I index) {
            this.index = index;
        }
        @Override
        public Iterator<Long> iterator() {
            return new PrimitiveLongKeyOffHeapViewIterator(index);
        }
    }
    
    public Iterable<Long> entriesForIndex(I index) {
        return new IndexIterable(index);
    }
    
    // iterator for nonunique keys, to get all entries.
    // direct call, avoids the temporary object creation of the Iterable
    public PrimitiveLongKeyOffHeapViewIterator iterator(I index) {
        return new PrimitiveLongKeyOffHeapViewIterator(index);
    }
    
    public class PrimitiveLongKeyOffHeapViewIterator implements Iterator<Long> {
        private long nextEntryPtr = 0L;     // we are "at End" if this field has value 0
        private long currentKey = 0L;       // updated from JNI
        
        private native long natIterateStart(long cStructOfMap, int hash, byte [] data, int length);
        private native long natIterate(long previousEntryPtr);
        
        /** Constructor, protected because it can only be created by the Map itself. */
        private PrimitiveLongKeyOffHeapViewIterator(I index) {
            if (converter == null) {
                nextEntryPtr = natIterateStart(cStruct, indexHash(index), null, 0);
            } else {
                byte [] data = converter.getBuffer(index); 
                nextEntryPtr = natIterateStart(cStruct, indexHash(index), data, converter.getLength());
            }
        }
        
        public long getCurrent() {
            if (currentKey <= 0L)
                throw new NoSuchElementException();
            return currentKey;
        }
        

        public long nextAsPrimitiveLong() {
            if (nextEntryPtr == 0L)
                throw new NoSuchElementException();
            long thisKey = currentKey;                  // currentKey will be modified during the JNI call
            nextEntryPtr = natIterate(nextEntryPtr);    // peek to the one after this (to allow removing the returned one)
            return thisKey;
        }
        
        @Override
        public boolean hasNext() {
            return nextEntryPtr != 0L;
        }

        @Override
        public Long next() {
            if (nextEntryPtr == 0L)
                throw new NoSuchElementException();
            long thisKey = currentKey;                  // currentKey will be modified during the JNI call
            nextEntryPtr = natIterate(nextEntryPtr);    // peek to the one after this (to allow removing the returned one)
            return Long.valueOf(thisKey);
        }
        
        @Override
        public void remove() {
            // not supported because with an index, maybe also unknown indexes need removal
            throw new UnsupportedOperationException();
        }
    }    
}
