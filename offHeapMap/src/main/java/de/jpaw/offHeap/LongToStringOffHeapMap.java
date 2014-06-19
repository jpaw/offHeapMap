package de.jpaw.offHeap;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import de.jpaw.collections.ByteArrayConverter;

/** Off heap storage for Strings. */
public class LongToStringOffHeapMap extends  AbstractPrimitiveLongKeyOffHeapMap<String> {
    
    private static class StringConverter implements ByteArrayConverter<String> {
        private final Charset encoding;
        private StringConverter(Charset encoding) {
            this.encoding = encoding;
        }
        
        @Override
        public byte [] valueTypeToByteArray(String arg) {
            return arg == null ? null : arg.getBytes(encoding);
        }

        @Override
        public String byteArrayToValueType(byte[] arg) {
            return arg != null ? new String(arg, encoding) : null;
        }
    };
    private static final StringConverter DEFAULT_CONVERTER = new StringConverter(StandardCharsets.UTF_8);

    protected LongToStringOffHeapMap(ByteArrayConverter<String> converter, int size, Shard forShard, int modes) {
        super(converter, size, forShard, modes);
    }

    public static class Builder extends AbstractPrimitiveLongKeyOffHeapMap.Builder<String, LongToStringOffHeapMap> {

        public Builder() {
            super(DEFAULT_CONVERTER);
        }
        public Builder(Charset encoding) {
            super(new StringConverter(encoding));
        }
        public LongToStringOffHeapMap build() {
            return new LongToStringOffHeapMap(converter, hashSize, shard, mode);
        }
    }
    
    // convenience constructor
    public static LongToStringOffHeapMap forHashSize(int hashSize) {
        return new Builder().setHashSize(hashSize).build();
    }

}
