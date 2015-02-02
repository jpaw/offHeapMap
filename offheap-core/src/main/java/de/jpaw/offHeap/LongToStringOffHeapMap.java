package de.jpaw.offHeap;

import de.jpaw.collections.ByteArrayConverter;

/** Off heap storage for Strings. */
public class LongToStringOffHeapMap extends  PrimitiveLongKeyOffHeapMap<String> {

    protected LongToStringOffHeapMap(ByteArrayConverter<String> converter, int size, Shard forShard, int modes, boolean withCommittedView) {
        super(converter, size, forShard, modes, withCommittedView);
    }

    public static class Builder extends PrimitiveLongKeyOffHeapMap.Builder<String, LongToStringOffHeapMap> {

        public Builder() {
            super(ByteArrayConverter.STRING_CONVERTER);
        }
        @Override
        public LongToStringOffHeapMap build() {
            return new LongToStringOffHeapMap(converter, hashSize, shard, mode, withCommittedView);
        }
    }
    
    // convenience constructor
    public static LongToStringOffHeapMap forHashSize(int hashSize) {
        return new Builder().setHashSize(hashSize).build();
    }

}
