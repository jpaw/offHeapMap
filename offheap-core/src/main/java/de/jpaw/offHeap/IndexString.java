package de.jpaw.offHeap;

import de.jpaw.collections.ByteArrayConverter;

public class IndexString extends PrimitiveLongKeyOffHeapIndex<String> {
    
    protected IndexString(ByteArrayConverter<String> converter, int size, Shard forShard, int modes, boolean withCommittedView, String name) {
        super(converter, size, forShard, modes, withCommittedView, name);
    }

    public static class Builder extends PrimitiveLongKeyOffHeapIndex.Builder<String, IndexString> {

        public Builder() {
            super(ByteArrayConverter.STRING_CONVERTER);
        }
        @Override
        public IndexString build() {
            return new IndexString(converter, hashSize, shard, mode, withCommittedView, name);
        }
    }
    
    // convenience constructor
    public static IndexString forHashSize(int hashSize) {
        return new Builder().setHashSize(hashSize).build();
    }
    // convenience constructor
    public static IndexString uniqueForHashSize(int hashSize) {
        return new Builder().setHashSize(hashSize).setUnique(true).build();
    }

}