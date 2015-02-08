package de.jpaw.collections;

import java.util.Iterator;

/** An iterator which also provides counterparts returning a primitive type,
 * to avoid unneccessary autoboxing.
 *  
 * @author Michael Bischoff
 *
 */
public interface PrimitiveLongIterator extends Iterator<Long> {
    
    /** Returns the next entry. */
    public long nextAsPrimitiveLong();
}
