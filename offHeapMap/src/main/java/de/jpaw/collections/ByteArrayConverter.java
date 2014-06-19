package de.jpaw.collections;

public interface ByteArrayConverter<V> {
    byte [] valueTypeToByteArray(V arg);
    V byteArrayToValueType(byte [] arg);
}
