package de.jpaw.offHeap;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import de.jpaw.collections.PrimitiveLongKeyMap;

/** Utility wrapper around LongToByteArrayOffHeapMap. */
public class LongToStringOffHeapMap implements PrimitiveLongKeyMap<String>, Iterable<PrimitiveLongKeyMap.Entry<String>> {

    private final Charset encoding;
    private final LongToByteArrayOffHeapMap map;
    
    public LongToStringOffHeapMap(LongToByteArrayOffHeapMap map, Charset encoding) {
        this.map = map;
        this.encoding = encoding;
    }
    
    public LongToStringOffHeapMap(LongToByteArrayOffHeapMap map) {
        this(map, StandardCharsets.UTF_8);
    }
    
    private String b2s(byte [] data) {
        return data != null ? new String(data, encoding) : null;
    }
    
    private byte [] s2b(String str) {
        return str == null ? null : str.getBytes(encoding);
    }
    
    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(long key) {
        return map.containsKey(key);
    }

    @Override
    public String get(long key) {
        return b2s(map.get(key));
    }

    @Override
    public String put(long key, String value) {
        return b2s(map.put(key, s2b(value)));
    }

    @Override
    public void set(long key, String value) {
        map.set(key, s2b(value));
    }

    @Override
    public String remove(long key) {
        return b2s(map.remove(key));
    }

    @Override
    public void delete(long key) {
        map.delete(key);
    }

    @Override
    public void clear() {
        map.clear();
    }

    public class Entry implements PrimitiveLongKeyMap.Entry<String> {
        private final long key;

        private Entry(long key) {
            this.key = key;
        }

        @Override
        public long getKey() {
            return key;
        }

        @Override
        public String getValue() {
            return b2s(map.get(key));
        }

        @Override
        public String setValue(String value) {
            return b2s(map.put(key,  s2b(value)));
        }
    }
    
    private class LongToStringOffHeapMapEntryIterator implements Iterator<PrimitiveLongKeyMap.Entry<String>> {
        private final Iterator<PrimitiveLongKeyMap.Entry<byte []>> delegate;
        
        private LongToStringOffHeapMapEntryIterator() {
            delegate = map.iterator();
        }
        
        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public de.jpaw.collections.PrimitiveLongKeyMap.Entry<String> next() {
            return new Entry(delegate.next().getKey());
        }
        
        @Override
        public void remove() {
            delegate.remove();
        }
    }
    
    @Override
    public Iterator<de.jpaw.collections.PrimitiveLongKeyMap.Entry<String>> iterator() {
        return new LongToStringOffHeapMapEntryIterator();
    }
    
    @Override
    public void putAll(PrimitiveLongKeyMap<? extends String> otherMap) {
        for (PrimitiveLongKeyMap.Entry<? extends String> otherEntry : otherMap)
            map.set(otherEntry.getKey(), s2b(otherEntry.getValue()));
    }

    
    // extras not in the interface
    void close() {
        map.close();
    }
}
