package de.jpaw.offHeap;

import de.jpaw.collections.ByteArrayConverter;

public class IndexLong extends PrimitiveLongKeyOffHeapIndex<Long> {
    
    protected IndexLong(ByteArrayConverter<Long> converter, int size, Shard forShard, int modes, boolean withCommittedView) {
        super(converter, size, forShard, modes, withCommittedView);
    }

    public static class Builder extends PrimitiveLongKeyOffHeapIndex.Builder<Long, IndexLong> {

        public Builder() {
            super(ByteArrayConverter.LONG_CONVERTER);
        }
        @Override
        public IndexLong build() {
            return new IndexLong(converter, hashSize, shard, mode, withCommittedView);
        }
    }
    
    // convenience constructor
    public static IndexLong forHashSize(int hashSize) {
        return new Builder().setHashSize(hashSize).build();
    }

}