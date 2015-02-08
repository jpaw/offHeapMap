package de.jpaw.collections;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public interface ByteArrayConverter<V> {
    /** Returns the value type for a given byte array. */
    V byteArrayToValueType(byte [] arg);
    
    /** Returns a byte array for the value. Usually this involves copying data, therefore use of getBuffer and getLength is preferred. */
    byte [] valueTypeToByteArray(V arg);
    
    /** Returns a byte array for the value, but the array may be too long. */
    byte [] getBuffer(V arg);
    
    /** Returns the length of the previous getBuffer operation and clears the internal temporary buffer. */
    int getLength();
    
    static public final Charset DEFAULT_CS = StandardCharsets.UTF_8;
    static public final ByteArrayConverter<String> STRING_CONVERTER = new ByteArrayConverter<String>() {
        private byte [] previous;
        
        @Override
        public byte[] valueTypeToByteArray(String arg) {
            return arg == null ? null : arg.getBytes(DEFAULT_CS);
        }
        @Override
        public String byteArrayToValueType(byte[] arg) {
            return arg == null ? null : new String(arg, DEFAULT_CS);
        }
        @Override
        public byte[] getBuffer(String arg) {
            previous = valueTypeToByteArray(arg);
            return previous;
        }
        @Override
        public int getLength() {
            int len = previous.length;
            previous = null;
            return len;
        }
    };
    
    // byte converter is actually a no op converter
    static public final ByteArrayConverter<byte []> BYTE_CONVERTER = new ByteArrayConverter<byte []>() {
        private int previousLen;
        
        @Override
        public byte[] valueTypeToByteArray(byte[] arg) {
            return arg;
        }
        @Override
        public byte[] byteArrayToValueType(byte[] arg) {
            return arg;
        }
        @Override
        public byte[] getBuffer(byte [] arg) {
            previousLen = arg.length;
            return arg;
        }
        @Override
        public int getLength() {
            return previousLen;
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
        @Override
        public byte[] getBuffer(Long arg) {
            return valueTypeToByteArray(arg);
        }
        @Override
        public int getLength() {
            return 8;
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
        @Override
        public byte[] getBuffer(UUID arg) {
            return valueTypeToByteArray(arg);
        }
        @Override
        public int getLength() {
            return 16;
        }
    };
}
