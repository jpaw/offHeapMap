package de.jpaw.offHeap;

import java.util.UUID;

import de.jpaw.collections.ByteArrayConverter;

public class IndexUUID extends PrimitiveLongKeyOffHeapIndex<UUID> {

    protected IndexUUID(ByteArrayConverter<UUID> converter, int size, Shard forShard, int modes, boolean withCommittedView, String name) {
        super(converter, size, forShard, modes, withCommittedView, name);
    }

    public static class Builder extends PrimitiveLongKeyOffHeapIndex.Builder<UUID, IndexUUID> {

        public Builder() {
            super(ByteArrayConverter.UUID_CONVERTER);
        }
        @Override
        public IndexUUID build() {
            return new IndexUUID(converter, hashSize, shard, mode, withCommittedView, name);
        }
    }

    // convenience constructor
    public static IndexUUID forHashSize(int hashSize) {
        return new Builder().setHashSize(hashSize).build();
    }

}
