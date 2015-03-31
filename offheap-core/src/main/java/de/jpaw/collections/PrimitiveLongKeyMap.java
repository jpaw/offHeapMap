package de.jpaw.collections;

/** An interface which is similar to java.util.Map, but for keys of type Java primitive long.
 * (The generic definition does only accept Object type keys.)
 * Additional write methods are defined here.
 *
 * @author Michael Bischoff
 *
 * @param <V>
 */
public interface PrimitiveLongKeyMap<V> extends PrimitiveLongKeyMapView<V> {

    // EXTRA! See hazelcast. put without returning value (in asynchronous environments, even the boolean might not be accurate)
    public boolean set(long key, V value);

    public V remove(long key);

    public void putAll(PrimitiveLongKeyMap<? extends V> m);

    public void clear();

    public PrimitiveLongKeyMapView<V> getView();
}
