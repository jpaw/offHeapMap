package de.jpaw.offHeap;

import de.jpaw.collections.ByteArrayConverter;

public class LongToByteArrayOffHeapMap extends PrimitiveLongKeyOffHeapMap<byte []> {
    
    private static final ByteArrayConverter<byte []> myConverter = new ByteArrayConverter<byte []>() {
        @Override
        public byte[] valueTypeToByteArray(byte[] arg) {
            return arg;
        }

        @Override
        public byte[] byteArrayToValueType(byte[] arg) {
            return arg;
        }
    };
    
    protected LongToByteArrayOffHeapMap(ByteArrayConverter<byte[]> converter, int size, Shard forShard, int modes, boolean withCommittedView) {
        super(converter, size, forShard, modes, withCommittedView);
    }

    public static class Builder extends PrimitiveLongKeyOffHeapMap.Builder<byte [], LongToByteArrayOffHeapMap> {

        public Builder() {
            super(myConverter);
        }
        @Override
        public LongToByteArrayOffHeapMap build() {
            return new LongToByteArrayOffHeapMap(converter, hashSize, shard, mode, withCommittedView);
        }
    }
    
    // convenience constructor
    public static LongToByteArrayOffHeapMap forHashSize(int hashSize) {
        return new Builder().setHashSize(hashSize).build();
    }

}
