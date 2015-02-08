package de.jpaw.offHeap;

import java.util.Iterator;
import java.util.NoSuchElementException;

import de.jpaw.collections.ByteArrayConverter;
import de.jpaw.collections.PrimitiveLongIterator;

/** All ooperations which are possible on a committed view onto an index. */
public class PrimitiveLongKeyOffHeapIndexView<I> extends AbstractOffHeapMap<I> {
    
    static {
        OffHeapInit.init();
        natInit(PrimitiveLongKeyOffHeapIndexView.PrimitiveLongKeyOffHeapViewIterator.class,
                PrimitiveLongKeyOffHeapIndexView.BatchedPrimitiveLongKeyOffHeapViewIterator.class);
    }
    
    // class can only be instantiated from a parent
    protected PrimitiveLongKeyOffHeapIndexView(
            long cMap,
            boolean isView,
            ByteArrayConverter<I> indexConverter,
            String name) {
        super(indexConverter, cMap, isView, name);
    }

//    protected byte [] indexData(I index) {
//        return converter == null ? null : converter.valueTypeToByteArray(index);
//    }
    
    protected int indexHash(I index) {
        return converter == null ? (Integer)index : index.hashCode();
    }

    /** Register globals (especially the Iterator class). */
    private static native void natInit(Class<?> iterator, Class<?> batchedIterator);

    /** Read an entry and return its key, or null if it does not exist. */
    private static native long natIndexGetKey(long cMap, int indexHash, byte [] indexData, int offset, int length);
    
    
    /** returns the key for an index or 0 if there is no entry for this key.
     * TODO: throws dup_val_on_index if there is no unique key. */
    public long getUniqueKeyByIndex(I index) {
        byte [] indexData = converter.getBuffer(index);
        return natIndexGetKey(cStruct, indexHash(index), indexData, 0, converter.getLength());
    }
    
    /** Returns the key for a given index entry, low level access method. */
    public long getUniqueKeyByIndex(byte [] indexData, int offset, int length, int hash) {
        return natIndexGetKey(cStruct, hash, indexData, offset, length);
    }
    
    public long getUniqueKeyByIndexDirect(int index) {
        return natIndexGetKey(cStruct, index, null, 0, 0);
    }
    
    private class IndexIterable implements Iterable<Long> {
        final I index;
        final int batchSize;
        private IndexIterable(I index, int batchSize) {
            this.index = index;
            this.batchSize = batchSize;
        }
        @Override
        public Iterator<Long> iterator() {
            return batchSize > 0
                ? new BatchedPrimitiveLongKeyOffHeapViewIterator(index, batchSize)
                : new PrimitiveLongKeyOffHeapViewIterator(index);
        }
    }
    
    public Iterable<Long> entriesForIndex(I index) {
        return new IndexIterable(index, 0);
    }
    
    public Iterable<Long> entriesForIndex(I index, int batchSize) {
        return new IndexIterable(index, batchSize);
    }
    
    // iterator for nonunique keys, to get all entries.
    // direct call, avoids the temporary object creation of the Iterable
    public PrimitiveLongKeyOffHeapViewIterator iterator(I index) {
        return new PrimitiveLongKeyOffHeapViewIterator(index);
    }
    
    public BatchedPrimitiveLongKeyOffHeapViewIterator iterator(I index, int batchSize) {
        return new BatchedPrimitiveLongKeyOffHeapViewIterator(index, batchSize);
    }
    
    public class PrimitiveLongKeyOffHeapViewIterator implements PrimitiveLongIterator {
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

        @Override
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
            return Long.valueOf(nextAsPrimitiveLong());
        }
        
        @Override
        public void remove() {
            // not supported because with an index, maybe also unknown indexes need removal
            throw new UnsupportedOperationException();
        }
    }    

    /** The batched iterator buffers N keys and therefore reduces the number of Java / JNI calls.
     * Only if it is know that at least 3 entries will exist (and be requested), this one should be preferred
     * over PrimitiveLongKeyOffHeapViewIterator.
     * 
     * The buffer is eagerly filled, i.e. whenever the last element has been requested, the next entries are
     * requested already.
     *  */
    public class BatchedPrimitiveLongKeyOffHeapViewIterator implements PrimitiveLongIterator {
        private long nextEntryPtr = 0L;     // we are "at End" if this field has value 0
        private int numValidEntries = 0;    // how may entries in nextEntries are valid? (filled via JNI)
        private int nextEntryToReturn = 0;  // index of the next entry to return
        private final long [] nextEntries;
        private final int batchSize;
        
        private native long natIterateStart(long cStructOfMap, int hash, byte [] data, int length, long [] entries, int batchSize);
        private native long natIterate(long previousEntryPtr, long [] entries, int batchSize);
        
        /** Constructor, protected because it can only be created by the Map itself. */
        private BatchedPrimitiveLongKeyOffHeapViewIterator(I index, int batchSize) {
            if (batchSize < 1) {
                throw new IllegalArgumentException("batch size must be at least 1, got " + batchSize);
            }
            nextEntries = new long [batchSize];
            this.batchSize = batchSize;
            if (converter == null) {
                nextEntryPtr = natIterateStart(cStruct, indexHash(index), null, 0, nextEntries, batchSize);
            } else {
                byte [] data = converter.getBuffer(index); 
                nextEntryPtr = natIterateStart(cStruct, indexHash(index), data, converter.getLength(), nextEntries, batchSize);
            }
        }
        
        private void getMore() {
            if (nextEntryPtr == 0)
                return;  // no need to try
            numValidEntries = 0;
            nextEntryToReturn = 0;
            nextEntryPtr = natIterate(nextEntryPtr, nextEntries, batchSize);    // peek to the one after this (to allow removing the returned one)
        }
        
        @Override
        public long nextAsPrimitiveLong() {
            if (nextEntryToReturn >= numValidEntries)
                throw new NoSuchElementException();
            long key =  nextEntries[nextEntryToReturn];
            if (++nextEntryToReturn == numValidEntries)
                getMore();
            return key;
        }
        
        @Override
        public boolean hasNext() {
            return nextEntryToReturn < numValidEntries;
        }

        @Override
        public Long next() {
            return Long.valueOf(nextAsPrimitiveLong());
        }
        
        @Override
        public void remove() {
            // not supported because with an index, maybe also unknown indexes need removal
            throw new UnsupportedOperationException();
        }
    }    
}
