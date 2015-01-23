package de.jpaw.offHeap;

import de.jpaw.collections.ByteArrayConverter;

/** All ooperations which are possible on a committed view onto an index. */
public class PrimitiveLongKeyOffHeapIndexView<V, I> extends AbstractOffHeapMap<I> {
    
    /** Only used by native code, to store the off heap address of the structure. */
    protected final PrimitiveLongKeyOffHeapMapView<V> dataMapView;  // the data view for this index
    
    // class can only be instantiated from a parent
    protected PrimitiveLongKeyOffHeapIndexView(
            PrimitiveLongKeyOffHeapMapView<V> dataMapView,
            long cMap,
            boolean isView,
            ByteArrayConverter<I> indexConverter) {
        super(indexConverter, cMap, isView);
        this.dataMapView = dataMapView;
    }

    protected byte [] indexData(I index) {
        return converter == null ? null : converter.valueTypeToByteArray(index);
    }
    
    protected int indexHash(I index) {
        return converter == null ? (Integer)index : index.hashCode();
    }
    
    
    
    /** Read an entry and return its key, or null if it does not exist. */
    private static native long natIndexGetKey(long cMap, int indexHash, byte [] indexData);

    /** Read an entry and return its value, or null if it does not exist. */
    private static native byte [] natIndexGetValue(long cMap, int indexHash, byte [] indexData);

    
    
    
    /** returns the key for an index or null if there is no entry for this key.
     * TODO: throws dup_val_on_index if there is no unique key. */
    public Long getUniqueKeyByIndex(I index) {
        long l = natIndexGetKey(cStruct, indexHash(index), indexData(index));
        return l == -1L ? null : Long.valueOf(l);
    }
    
    /** Returns the data object indexed by index, or null if it does not exist. */
    public V getUniqueValueByIndex(I index) {
        return dataMapView.converter.byteArrayToValueType(natIndexGetValue(cStruct, indexHash(index), indexData(index)));
    }
    public Long getUniqueKeyByIndexDirect(int index) {
        long l = natIndexGetKey(cStruct, index, null);
        return l == -1L ? null : Long.valueOf(l);
    }
    
    /** Returns the data object indexed by index, or null if it does not exist. */
    public V getUniqueValueByIndexDirect(int index) {
        return dataMapView.converter.byteArrayToValueType(natIndexGetValue(cStruct, index, null));
    }
}
