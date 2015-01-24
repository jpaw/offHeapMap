package de.jpaw.collections;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

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
    
    // byte converter is actually a no op converter
    static public final ByteArrayConverter<byte []> BYTE_CONVERTER = new ByteArrayConverter<byte []>() {
        @Override
        public byte[] valueTypeToByteArray(byte[] arg) {
            return arg;
        }

        @Override
        public byte[] byteArrayToValueType(byte[] arg) {
            return arg;
        }
    };
    
    // see http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/primitives/Longs.html or
    // http://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java
    static public final ByteArrayConverter<Long> LONG_CONVERTER = new ByteArrayConverter<Long>() {
        @Override
        public byte[] valueTypeToByteArray(Long arg) {
            return arg == null ? null : ByteBuffer.allocate(8).putLong(arg).array();
        }

        @Override
        public Long byteArrayToValueType(byte[] arg) {
            return arg == null ? null : ByteBuffer.wrap(arg).getLong();
        }
    };
    
    static public final ByteArrayConverter<UUID> UUID_CONVERTER = new ByteArrayConverter<UUID>() {
        @Override
        public byte[] valueTypeToByteArray(UUID arg) {
            return arg == null ? null
                : ByteBuffer.allocate(8)
                    .putLong(arg.getMostSignificantBits())
                    .putLong(arg.getLeastSignificantBits())
                    .array();
        }

        @Override
        public UUID byteArrayToValueType(byte[] arg) {
            if (arg == null)
                return null;
            final ByteBuffer b = ByteBuffer.wrap(arg);
            final long msb = b.getLong();
            final long lsb = b.getLong();
            return new UUID(msb, lsb); 
        }
    };
}
