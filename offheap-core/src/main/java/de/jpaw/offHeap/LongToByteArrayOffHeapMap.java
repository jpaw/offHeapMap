package de.jpaw.offHeap;

import de.jpaw.collections.ByteArrayConverter;

public class LongToByteArrayOffHeapMap extends PrimitiveLongKeyOffHeapMap<byte []> {
    
    protected LongToByteArrayOffHeapMap(ByteArrayConverter<byte[]> converter, int size, Shard forShard, int modes, boolean withCommittedView, String name) {
        super(converter, size, forShard, modes, withCommittedView, name);
    }

    public static class Builder extends PrimitiveLongKeyOffHeapMap.Builder<byte [], LongToByteArrayOffHeapMap> {

        public Builder() {
            super(ByteArrayConverter.BYTE_CONVERTER);
        }
        @Override
        public LongToByteArrayOffHeapMap build() {
            return new LongToByteArrayOffHeapMap(converter, hashSize, shard, mode, withCommittedView, name);
        }
    }
    
    // convenience constructor
    public static LongToByteArrayOffHeapMap forHashSize(int hashSize) {
        return new Builder().setHashSize(hashSize).build();
    }

}
