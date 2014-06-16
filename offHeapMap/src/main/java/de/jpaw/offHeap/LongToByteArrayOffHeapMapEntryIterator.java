package de.jpaw.offHeap;

import java.util.Iterator;
import java.util.NoSuchElementException;

import de.jpaw.collections.PrimitiveLongKeyMap;

public class LongToByteArrayOffHeapMapEntryIterator implements Iterator<PrimitiveLongKeyMap.Entry<byte []>> {
    private final LongToByteArrayOffHeapMap map;
    private final long cStructOfMap;
    
    private LongToByteArrayOffHeapMapEntry currentEntry = null; // the cached last entry
    
    private long nextEntryPtr = 0L;     // we are "at End" if this field has value 0
    private long currentKey = 0L;       // updated from JNI
    private int currentHashIndex = 0;   // updated from JNI
    
    static {
        OffHeapInit.init();
        natInit();
    }
    
    /** Register globals. */
    private native static void natInit();
    
    private native long natIterate(long cStructOfMap, long previousEntryPtr, int currentHashIndex);
    
    
    /** Constructor, protected because it can only be created by the Map itself. */
    protected LongToByteArrayOffHeapMapEntryIterator(LongToByteArrayOffHeapMap map, long cStructOfMap) {
        this.map = map;
        this.cStructOfMap = cStructOfMap;
        nextEntryPtr = natIterate(cStructOfMap, 0L, -1);   // true = isInitial
    }
    
    public LongToByteArrayOffHeapMapEntry getCurrent() {
        if (currentEntry == null)
            throw new NoSuchElementException();
        return currentEntry;
    }
    
    @Override
    public boolean hasNext() {
        return nextEntryPtr != 0L;
    }

    @Override
    public LongToByteArrayOffHeapMapEntry next() {
        if (nextEntryPtr == 0L)
            throw new NoSuchElementException();
        currentEntry = new LongToByteArrayOffHeapMapEntry(currentKey, map);
        nextEntryPtr = natIterate(cStructOfMap, nextEntryPtr, currentHashIndex);   // peek to the one after this (to allow removing the returned one)
        return currentEntry;
    }
    
    @Override
    public void remove() {
        if (currentEntry == null)
            throw new NoSuchElementException();
        map.delete(currentEntry.getKey());
    }
}
