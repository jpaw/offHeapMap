package de.jpaw.offHeap;

import java.util.Iterator;
import java.util.NoSuchElementException;

import de.jpaw.collections.ByteArrayConverter;

/** All ooperations which are possible on a committed view onto an index. */
public class PrimitiveLongKeyOffHeapIndexView<I> extends AbstractOffHeapMap<I> {
    
    static {
        OffHeapInit.init();
        natInit(PrimitiveLongKeyOffHeapIndexView.PrimitiveLongKeyOffHeapMapEntryIterator.class);
    }
    
    // class can only be instantiated from a parent
    protected PrimitiveLongKeyOffHeapIndexView(
            long cMap,
            boolean isView,
            ByteArrayConverter<I> indexConverter) {
        super(indexConverter, cMap, isView);
    }

    protected byte [] indexData(I index) {
        return converter == null ? null : converter.valueTypeToByteArray(index);
    }
    
    protected int indexHash(I index) {
        return converter == null ? (Integer)index : index.hashCode();
    }

    /** Register globals (especially the Iterator class). */
    private static native void natInit(Class<?> arg);

    /** Read an entry and return its key, or null if it does not exist. */
    private static native long natIndexGetKey(long cMap, int indexHash, byte [] indexData);
    
    
    /** returns the key for an index or null if there is no entry for this key.
     * TODO: throws dup_val_on_index if there is no unique key. */
    public Long getUniqueKeyByIndex(I index) {
        long l = natIndexGetKey(cStruct, indexHash(index), indexData(index));
        return l == -1L ? null : Long.valueOf(l);
    }
    
    public Long getUniqueKeyByIndexDirect(int index) {
        long l = natIndexGetKey(cStruct, index, null);
        return l == -1L ? null : Long.valueOf(l);
    }
    
    private class IndexIterable implements Iterable<Long> {
        final I index;
        private IndexIterable(I index) {
            this.index = index;
        }
        @Override
        public Iterator<Long> iterator() {
            return new PrimitiveLongKeyOffHeapMapEntryIterator(indexHash(index), indexData(index));
        }
    }
    
    public Iterable<Long> entriesForIndex(I index) {
        return new IndexIterable(index);
    }
    
    // iterator for nonunique keys, to get all entries.
    // direct call, avoids the temporary object creation of the Iterable
    public Iterator<Long> iterator(I index) {
        return new PrimitiveLongKeyOffHeapMapEntryIterator(indexHash(index), indexData(index));
    }
    
    private class PrimitiveLongKeyOffHeapMapEntryIterator implements Iterator<Long> {
        private Long currentEntry = null;
        
        private long nextEntryPtr = 0L;     // we are "at End" if this field has value 0
        private long currentKey = 0L;       // updated from JNI
        
        private native long natIterateStart(long cStructOfMap, int hash, byte [] data);
        private native long natIterate(long previousEntryPtr);
        
        /** Constructor, protected because it can only be created by the Map itself. */
        protected PrimitiveLongKeyOffHeapMapEntryIterator(int hash, byte [] data) {
            nextEntryPtr = natIterateStart(cStruct, hash, data);   // true = isInitial
        }
        
        public Long getCurrent() {
            if (currentEntry == null)
                throw new NoSuchElementException();
            return currentEntry;
        }
        
        @Override
        public boolean hasNext() {
            return nextEntryPtr != 0L;
        }

        @Override
        public Long next() {
            if (nextEntryPtr == 0L)
                throw new NoSuchElementException();
            currentEntry = Long.valueOf(currentKey);
            nextEntryPtr = natIterate(nextEntryPtr);   // peek to the one after this (to allow removing the returned one)
            return currentEntry;
        }
        
        @Override
        public void remove() {
            // not supported because with an index, maybe also unknown indexes need removal
            throw new UnsupportedOperationException();
        }
    }    
}
