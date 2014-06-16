package de.jpaw.offHeap;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** Off heap storage for Strings. */
public class LongToStringOffHeapMap extends  AbstractPrimitiveLongKeyOffHeapMap<String> {

    private Charset encoding = StandardCharsets.UTF_8;
    
    public Charset getEncoding() {
        return encoding;
    }

    public void setEncoding(Charset encoding) {
        this.encoding = encoding;
    }

    public LongToStringOffHeapMap(int size, Shard forShard, int modes) {
        super(size, forShard, modes);
    }
    public LongToStringOffHeapMap(int size, Shard forShard) {
        super(size, forShard);
    }
    public LongToStringOffHeapMap(int size) {
        super(size);
    }

    @Override
    protected byte [] valueTypeToByteArray(String arg) {
        return arg == null ? null : arg.getBytes(encoding);
    }

    @Override
    protected String byteArrayToValueType(byte[] arg) {
        return arg != null ? new String(arg, encoding) : null;
    }
}
