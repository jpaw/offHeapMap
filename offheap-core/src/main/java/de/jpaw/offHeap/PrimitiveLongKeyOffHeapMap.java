package de.jpaw.offHeap;

import java.nio.charset.Charset;

import de.jpaw.collections.ByteArrayConverter;
import de.jpaw.collections.DatabaseIO;
import de.jpaw.collections.PrimitiveLongKeyMap;

public class PrimitiveLongKeyOffHeapMap<V> extends PrimitiveLongKeyOffHeapMapView<V>
implements PrimitiveLongKeyMap<V>, DatabaseIO {
    
    protected final Shard myShard;
    
    protected final PrimitiveLongKeyOffHeapMapView<V> myView;
    
    /** The threshold at which entries stored should be automatically compressed, in bytes.
     * It can be changed on the fly, at any time, using it to compress only specific items.
     * Setting it to Integer.MAX_VALUE will disable compression. Setting it to 0 will perform compression for all (non-zero-length) items. */
    private int maxUncompressedSize = Integer.MAX_VALUE;
    
    /** Removes an entry from the map. returns true if it was removed, false if no data was present. */
    private static native boolean natDelete(long cMap, long ctx, long key);
    
    /** Read an entry and return it in uncompressed form, then deletes it from the structure.
     *  Returns null if no entry is present for the specified key. */
    private static native byte [] natRemove(long cMap, long ctx, long key);
    
    /** Stores an entry in the map. Returns true if this was a new entry. Returns false if the operation has overwritten existing data.
     * The new data will be compressed if indicated by the last parameter. data may not be null (use remove(key) for that purpose). */
    private static native boolean natSet(long cMap, long ctx, long key, byte [] data, int offset, int length, boolean doCompress);
    
    /** Stores an entry in the map and returns the previous entry, or null if there was no prior entry for this key.
     * data may not be null (use get(key) for that purpose). */
    private static native byte [] natPut(long cMap, long ctx, long key, byte [] data, int offset, int length, boolean doCompress);
    
    protected boolean shouldICompressThis(byte [] data) {
        return data.length > maxUncompressedSize;
    }


    
    // TODO: use the builder pattern here, the number of optional parameters is growing...
    protected PrimitiveLongKeyOffHeapMap(ByteArrayConverter<V> converter, int size, Shard forShard, int modes, boolean withCommittedView) {
        super(converter, natOpen(size, modes, withCommittedView), false);
        myShard = forShard;
        myView = withCommittedView ? new PrimitiveLongKeyOffHeapMapView<V>(converter, natGetView(cStruct), true) : null;
    }
    
    public abstract static class Builder<V, T extends PrimitiveLongKeyOffHeapMap<V>> {
        protected final ByteArrayConverter<V> converter;
        protected int hashSize = 4096;
        protected int mode = 0x81;
        protected Shard shard = Shard.TRANSACTIONLESS_DEFAULT_SHARD;
        protected boolean withCommittedView = false;
        
        public Builder(ByteArrayConverter<V> converter) {
            this.converter = converter;
        }
        public Builder<V, T> setHashSize(int hashSize) {
            this.hashSize = hashSize;
            return this;
        }
        public Builder<V, T> setShard(Shard shard) {
            this.shard = shard;
            return this;
        }
        public Builder<V, T> setAutonomous() {
            this.mode = 0;
            return this;
        }
        public Builder<V, T> addCommittedView() {
            this.withCommittedView = true;
            return this;
        }
        public abstract T build();
    }
    
    /** Dump the full database contents to a disk file. */
    @Override
    public void writeToFile(String pathname, Charset filenameEncoding) {
        natWriteToFile(cStruct, pathname.getBytes(filenameEncoding == null ? DEFAULT_FILENAME_ENCODING : filenameEncoding), false);
    }
    
    /** Read a database from disk. The database should be empty before. */
    @Override
    public void readFromFile(String pathname, Charset filenameEncoding) {
        natReadFromFile(cStruct, pathname.getBytes(filenameEncoding == null ? DEFAULT_FILENAME_ENCODING : filenameEncoding));
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
        natClose(cStruct);
//        cStruct = 0L;
    }
    
    /** Deletes all entries from the map, but keeps the map structure itself. */
    @Override
    public void clear() {
        natClear(cStruct, myShard.getTxCStruct());
    }

    /** Removes the entry stored for key from the map (if it did exist). */
    @Override
    public void delete(long key) {
        natDelete(cStruct, myShard.getTxCStruct(), key);
    }
    
    /** Stores an entry in the map.
     * The new data will be compressed if it is bigger than the threshold passed in map creation.
     * Deleting an entry can be done by passing null as the data pointer. */
    @Override
    public void set(long key, V data) {
        if (data == null) {
            delete(key);
        } else {
            byte [] arr = converter.valueTypeToByteArray(data);
            natSet(cStruct, myShard.getTxCStruct(), key, arr, 0, arr.length, shouldICompressThis(arr));
        }
    }
    
    /** Stores an entry in the map and returns the previous entry, or null if there was no prior entry for this key.
     * Deleting an entry can be done by passing null as the data pointer. */
    @Override
    public V put(long key, V data) {
        if (data == null) {
            return converter.byteArrayToValueType(natRemove(cStruct, myShard.getTxCStruct(), key));
        } else {
            byte [] arr = converter.valueTypeToByteArray(data);
            return converter.byteArrayToValueType(natPut(cStruct, myShard.getTxCStruct(), key, arr, 0, arr.length, shouldICompressThis(arr)));
        }
    }
    
    /** Stores an entry in the map, specified by a region within a byte array.
     * The new data will be compressed if it is bigger than the threshold passed in map creation.
     * Deleting an entry can be done by passing null as the data pointer. */
    public void setFromBuffer(long key, byte [] data, int offset, int length) {
        if (data == null || offset < 0 || offset + length > data.length)
            throw new IllegalArgumentException();
        natSet(cStruct, myShard.getTxCStruct(), key, data, offset, length, shouldICompressThis(data));
    }


    @Override
    public V remove(long key) {
        return converter.byteArrayToValueType(natRemove(cStruct, myShard.getTxCStruct(), key));
    }

    @Override
    public void putAll(PrimitiveLongKeyMap<? extends V> otherMap) {
        for (PrimitiveLongKeyMap.Entry<? extends V> otherEntry : otherMap) {
            set(otherEntry.getKey(), otherEntry.getValue());
        }
    }
    
    @Override
    public PrimitiveLongKeyOffHeapMapView<V> getView() {
        return myView;
    }

}
