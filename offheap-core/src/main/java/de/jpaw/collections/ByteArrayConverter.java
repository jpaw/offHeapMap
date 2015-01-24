package de.jpaw.collections;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public interface ByteArrayConverter<V> {
    byte [] valueTypeToByteArray(V arg);
    V byteArrayToValueType(byte [] arg);
    
    static public final Charset DEFAULT_CS = StandardCharsets.UTF_8;
    static public final ByteArrayConverter<String> STRING_CONVERTER = new ByteArrayConverter<String>() {

        @Override
        public byte[] valueTypeToByteArray(String arg) {
            return arg == null ? null : arg.getBytes(DEFAULT_CS);
        }
        @Override
        public String byteArrayToValueType(byte[] arg) {
            return arg == null ? null : new String(arg, DEFAULT_CS);
        }
    };
}
