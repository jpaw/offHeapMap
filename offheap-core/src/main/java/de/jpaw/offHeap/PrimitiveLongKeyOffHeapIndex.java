package de.jpaw.offHeap;

import de.jpaw.collections.ByteArrayConverter;

public class PrimitiveLongKeyOffHeapIndex<I> extends PrimitiveLongKeyOffHeapIndexView<I> {
    protected final PrimitiveLongKeyOffHeapIndexView<I> myView;
    
    protected final Shard myShard;
    
    // TODO: use the builder pattern here, the number of optional parameters is growing...
    public PrimitiveLongKeyOffHeapIndex(
            ByteArrayConverter<I> indexConverter,
            int size, Shard forShard, int modes, boolean withCommittedView) {
        // construct the map index map
        super(natOpen(size, modes, withCommittedView), false, indexConverter);
        
        // register at transaction for the same shard
        myShard = forShard;
        myView = withCommittedView ? new PrimitiveLongKeyOffHeapIndexView<I>(natGetView(cStruct), true, indexConverter) : null;
    }
    
    public PrimitiveLongKeyOffHeapIndex(
            ByteArrayConverter<I> indexConverter,
            int size, int modes) {
        this(indexConverter, size, Shard.TRANSACTIONLESS_DEFAULT_SHARD, modes, false);
    }
    
    public abstract static class Builder<I, T extends PrimitiveLongKeyOffHeapIndex<I>> {
        protected final ByteArrayConverter<I> converter;
        protected int hashSize = 4096;
        protected int mode = 0xa1;
        protected Shard shard = Shard.TRANSACTIONLESS_DEFAULT_SHARD;
        protected boolean withCommittedView = false;
        
        public Builder(ByteArrayConverter<I> converter) {
            this.converter = converter;
        }
        public Builder<I, T> setHashSize(int hashSize) {
            this.hashSize = hashSize;
            return this;
        }
        public Builder<I, T> setShard(Shard shard) {
            this.shard = shard;
            return this;
        }
        public Builder<I, T> setUnique(boolean unique) {
            if (unique)
                this.mode |= 0x10;
            else
                this.mode &= ~0x10;
            return this;
        }
        public Builder<I, T> setAutonomous() {
            this.mode &= ~0x81;
            return this;
        }
        public Builder<I, T> addCommittedView() {
            this.withCommittedView = true;
            return this;
        }
        public abstract T build();
    }
    
    
    /** Read an entry and return its key, or null if it does not exist. */
    private static native void natIndexCreate(long cMap, long ctx, long key, int indexHash, byte [] indexData, int length);

    /** Read an entry and return its key, or null if it does not exist. indexHash provided just for plausi check. */
    private static native void natIndexDelete(long cMap, long ctx, long key, int indexHash);

    /** Update some existing key with a new one. The old and new are different, this has been checked by the caller.
     * oldKeyHash provided just for plausi check. */
    private static native void natIndexUpdate(long cMap, long ctx, long key, int oldKeyHash, int newKeyHash, byte [] newKeyData, int length);


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
                byte [] indexData = converter.getBuffer(newIndex);
                natIndexCreate(cStruct, myShard.getTxCStruct(), key, indexHash(newIndex), indexData, converter.getLength());
            }
        } else {
            int oldHash = indexHash(oldIndex);
            if (newIndex == null) {
                natIndexDelete(cStruct, myShard.getTxCStruct(), key, oldHash);
            } else {
                int newHash = indexHash(newIndex);
                if (oldHash != newHash || oldIndex.equals(newIndex)) {
                    // only invoke the native method if old and new key are different
                    byte [] indexData = converter.getBuffer(newIndex);
                    natIndexUpdate(cStruct, myShard.getTxCStruct(), key, oldHash, newHash, indexData, converter.getLength());
                }
            }
        }
    }
    
    /** Update the key, I is a max 32 bit primitive type which cannot be null. */
    public void updateDirect(long key, int oldIndex, int newIndex) {
        if (oldIndex != newIndex)
            natIndexUpdate(cStruct, myShard.getTxCStruct(), key, oldIndex, newIndex, null, 0);
    }
    
    public void create(long key, I index) {
        if (index != null) {
            byte [] indexData = converter.getBuffer(index);
            natIndexCreate(cStruct, myShard.getTxCStruct(), key, indexHash(index), indexData, converter.getLength());
        }
    }
    public void createDirect(long key, int index) {
        natIndexCreate(cStruct, myShard.getTxCStruct(), key, index, null, 0);
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
    
    public PrimitiveLongKeyOffHeapIndexView<I> getView() {
        return myView;
    }
}
