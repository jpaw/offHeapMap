package de.jpaw.offHeap;

import de.jpaw.collections.ByteArrayConverter;

public class PrimitiveLongKeyOffHeapIndex<V,I> extends PrimitiveLongKeyOffHeapIndexView<V,I> {
    protected final PrimitiveLongKeyOffHeapIndexView<V,I> myView;
    
    protected final Shard myShard;
    
    // TODO: use the builder pattern here, the number of optional parameters is growing...
    protected PrimitiveLongKeyOffHeapIndex(
            PrimitiveLongKeyOffHeapMap<V> dataMap,
            ByteArrayConverter<I> indexConverter,
            int size,
            int modes) {
        // construct the map index map
        super(dataMap, natOpen(size, modes, dataMap.myView != null, dataMap.cStruct), false, indexConverter);
        
        // register at transaction for the same shard
        myShard = dataMap.myShard;
        myView = dataMap.myView != null ? new PrimitiveLongKeyOffHeapIndexView<V,I>(dataMap.getView(), natGetView(cStruct), true, indexConverter) : null;
    }
    
    /** Read an entry and return its key, or null if it does not exist. */
    private static native void natIndexCreate(long cMap, long ctx, long key, int indexHash, byte [] indexData);

    /** Read an entry and return its key, or null if it does not exist. indexHash provided just for plausi check. */
    private static native void natIndexDelete(long cMap, long ctx, long key, int indexHash);

    /** Update some existing key with a new one. The old and new are different, this has been checked by the caller.
     * oldKeyHash provided just for plausi check. */
    private static native void natIndexUpdate(long cMap, long ctx, long key, int oldKeyHash, int newKeyHash, byte [] newKeyData);


    /** Update the key entry for the data record of artificial key "key" from "oldIndex" to "newIndex".
     * Both "oldIndex" and "newIndex" may be null 
     * @param key
     * @param oldIndex
     * @param newIndex
     * 
     * throws DuplicateKeyException if the index is flagged as unique and the key exists already before.
     * throws InconsistentIndexException if for remove or update, no old entry of matching content could be found
     */
    public void update(long key, I oldIndex, I newIndex) {
        if (oldIndex == null) {
            // insert or no-op operation
            if (newIndex != null) {
                natIndexCreate(cStruct, myShard.getTxCStruct(), key, indexHash(newIndex), indexData(newIndex));
            }
        } else {
            int oldHash = indexHash(oldIndex);
            if (newIndex == null) {
                natIndexDelete(cStruct, myShard.getTxCStruct(), key, oldHash);
            } else {
                int newHash = indexHash(newIndex);
                if (oldHash != newHash || oldIndex.equals(newIndex))
                    // only invoke the native method if old and new key are different
                    natIndexUpdate(cStruct, myShard.getTxCStruct(), key, oldHash, newHash, indexData(newIndex));
            }
        }
    }
    
    /** Update the key, I is a max 32 bit primitive type which cannot be null. */
    public void updateDirect(long key, int oldIndex, int newIndex) {
        if (oldIndex != newIndex)
            natIndexUpdate(cStruct, myShard.getTxCStruct(), key, oldIndex, newIndex, null);
    }
    
    public void create(long key, I index) {
        if (index != null) {
            natIndexCreate(cStruct, myShard.getTxCStruct(), key, indexHash(index), indexData(index));
        }
    }
    public void createDirect(long key, int index) {
        natIndexCreate(cStruct, myShard.getTxCStruct(), key, index, null);
    }
    
    public void delete(long key, I index) {
        if (index != null) {
            natIndexDelete(cStruct, myShard.getTxCStruct(), key, indexHash(index));
        }
    }
    public void deleteDirect(long key, int index) {
        natIndexDelete(cStruct, myShard.getTxCStruct(), key, index);
    }
    /** Closes the index (clears the map and all entries it from memory).
     * After close() has been called, the object should not be used any more. */
    public void close() {
        natClose(cStruct);
//        cStruct = 0L;
    }
    
    public PrimitiveLongKeyOffHeapIndexView<V,I> getView() {
        return myView;
    }
}
