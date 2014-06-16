package de.jpaw.offHeap;

import de.jpaw.collections.PrimitiveLongKeyMap;

/** The Map.entry is just a proxy to the real data (flyweight). The value is obtained from the backing store on demand.
 * An optimized version could store a direct pointer to the off-Heap data, avoiding the hash computation, or even cache the value
 * in case multiple accesses are done. */
public class LongToByteArrayOffHeapMapEntry implements PrimitiveLongKeyMap.Entry<byte []> {
    private final long key;
    private final LongToByteArrayOffHeapMap map;
    
    protected LongToByteArrayOffHeapMapEntry(long key, LongToByteArrayOffHeapMap map) {
        this.key = key;
        this.map = map;
    }

    @Override
    public long getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return map.get(key);
    }

    @Override
    public byte[] setValue(byte[] value) {
        return map.put(key, value);
    }
}
