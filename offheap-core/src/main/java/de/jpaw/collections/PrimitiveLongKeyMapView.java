package de.jpaw.collections;

import java.util.Iterator;

public interface PrimitiveLongKeyMapView<V> extends OffHeapBaseMap, Iterable<PrimitiveLongKeyMapView.Entry<V>> {
    interface Entry<V> {
        public long getKey();
        public V getValue();
        public V setValue(V value);
    }

    public boolean containsKey(long key);

    // this one could be slow or not supported!
//    public boolean containsValue(Object value);

    public V get(long key);

    // only defined in order to support Entry.setValue()
    public V put(long key, V value);

    // EXTRA! See hazelcast. remove without returning value
    public void delete(long key);       // this throws an exception if applied to a view!. It's here to support the Iterator

//    public PrimitiveLongSet keySet();
//    public Collection<V> values();
//    public Set<PrimitiveLongKeyMap.Entry<V>> entrySet();

    // EXTRA!
    @Override
    public Iterator<PrimitiveLongKeyMapView.Entry<V>> iterator();
}
