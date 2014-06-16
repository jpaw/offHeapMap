package de.jpaw.collections;

//import java.util.Collection;
import java.util.Iterator;
//import java.util.Set;

/** An interface which is similar to java.util.Map, but for keys of type Java primitive long.
 * (The generic definition does only accept Object type keys.)
 * 
 * @author Michael Bischoff
 *
 * @param <V>
 */
public interface PrimitiveLongKeyMap<V> extends Iterable<PrimitiveLongKeyMap.Entry<V>> {
    interface Entry<V> {
        public boolean equals(Object o);
        public int hashCode();
        public long getKey();
        public V getValue();
        public V setValue(V value);
    }

    public int size();

    public boolean isEmpty();

    public boolean containsKey(long key);

    // this one could be slow or not supported!
//    public boolean containsValue(Object value);

    public V get(long key);

    public V put(long key, V value);

    // EXTRA! See hazelcast. put without returning value
    public void set(long key, V value);
    
    public V remove(long key);

    // EXTRA! See hazelcast. remove without returning value
    public void delete(long key);
    
    public void putAll(PrimitiveLongKeyMap<? extends V> m);

    public void clear();

//    public PrimitiveLongSet keySet();
//    public Collection<V> values();
//    public Set<PrimitiveLongKeyMap.Entry<V>> entrySet();

    // EXTRA!
    public Iterator<PrimitiveLongKeyMap.Entry<V>> iterator();
}
