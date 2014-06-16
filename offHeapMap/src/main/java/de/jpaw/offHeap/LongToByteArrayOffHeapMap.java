package de.jpaw.offHeap;

public class LongToByteArrayOffHeapMap extends AbstractPrimitiveLongKeyOffHeapMap<byte []> {

    public LongToByteArrayOffHeapMap(int size, Shard forShard, int modes) {
        super(size, forShard, modes);
    }
    public LongToByteArrayOffHeapMap(int size, Shard forShard) {
        super(size, forShard);
    }
    public LongToByteArrayOffHeapMap(int size) {
        super(size);
    }

    @Override
    protected byte[] valueTypeToByteArray(byte[] arg) {
        return arg;
    }

    @Override
    protected byte[] byteArrayToValueType(byte[] arg) {
        return arg;
    }

}
