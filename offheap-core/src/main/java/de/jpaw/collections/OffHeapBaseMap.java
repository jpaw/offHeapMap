package de.jpaw.collections;

public interface OffHeapBaseMap {

    /** Returns true, if this is a readonly view, false if it is extended by a superclass which provides R/W functionality
     * (i.e. Entry.setValue() is supported and Iterator.remove()).
     */
    public boolean isReadonly();

    public int size();

    public boolean isEmpty();

}
