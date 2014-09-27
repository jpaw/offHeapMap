package de.jpaw.collections;

import java.util.Iterator;

public interface PrimitiveLongKeyMapView<V> extends Iterable<PrimitiveLongKeyMapView.Entry<V>> {
    interface Entry<V> {
        @Override
        public boolean equals(Object o);
        @Override
        public int hashCode();
        public long getKey();
        public V getValue();
        public V setValue(V value);
    }

    /** Returns true, if this is a readonly view, false if it is extended by a superclass which provides R/W functionality
     * (i.e. Entry.setValue() is supported and Iterator.remove()).
     */
    public boolean isReadonly();
    
    public int size();

    public boolean isEmpty();

    public boolean containsKey(long key);

    // this one could be slow or not supported!
//    public boolean containsValue(Object value);

    public V get(long key);

    // only defined in order to support Entry.setValue()
    public V put(long key, V value);

    // EXTRA! See hazelcast. remove without returning value
    public void delete(long key);

//    public PrimitiveLongSet keySet();
//    public Collection<V> values();
//    public Set<PrimitiveLongKeyMap.Entry<V>> entrySet();

    // EXTRA!
    @Override
    public Iterator<PrimitiveLongKeyMapView.Entry<V>> iterator();
}
