package de.jpaw.offHeap;

import java.util.Iterator;

import org.testng.annotations.Test;

import de.jpaw.collections.PrimitiveLongKeyMap;
import de.jpaw.collections.PrimitiveLongKeyMap.Entry;


@Test
public class IteratorTest {
    static public final byte [] b1 = "1 One".getBytes();
    static public final byte [] b2 = "2 Two".getBytes();
    static public final byte [] b3 = "3 Three".getBytes();
    static public final byte [] b4 = "4 Four".getBytes();
    static public final long keys[] = { 12312L, 23423L, 6166L, 182638L };
    static public final byte [] data[] = { b1, b2, b3, b4 };
    
    public void runIteratorTest() {
        int i;
        PrimitiveLongKeyMap<byte[]> myMap = new LongToByteArrayOffHeapMap(8);
        assert(data[0] == b1);
        assert(data[1] == b2);
        assert(data[2] == b3);
        assert(data[3] == b4);
        
        for (i = 0; i < 4; ++i)
            myMap.set(keys[i], data[i]);
        
        // explicit
//        Iterator<Entry<byte[]>> iter = myMap.iterator();
//        boolean hasAtLeastOne = iter.hasNext();
//        Entry<byte[]> e1 = iter.next();
        
        for (Entry<byte[]> e : myMap) {
            System.out.println("Got <" + e.getKey() + ", " + new String(e.getValue()) + ">");
        }
        
        myMap.clear();
    }

    private void runWithDelete(Iterator<Entry<byte[]>> iter, String msg) {
        System.out.println(msg);
        int i;
        for (i = 0; iter.hasNext(); ++i) {
            Entry<byte[]> e = iter.next();
            System.out.println("Got <" + e.getKey() + ", " + new String(e.getValue()) + ">");
            if (i == 1)
                iter.remove();
        }
    }
    
    public void runIteratorWithDeleteTest() {
        int i;
        PrimitiveLongKeyMap<byte[]> myMap = new LongToByteArrayOffHeapMap(8);
        
        for (i = 0; i < 4; ++i)
            myMap.set(keys[i], data[i]);
        
        // using explicit iterator
        runWithDelete(myMap.iterator(), "initial run");
        runWithDelete(myMap.iterator(), "run after deleted first element");
        runWithDelete(myMap.iterator(), "run after deleted second element");
        runWithDelete(myMap.iterator(), "run after deleted third element");  // this run does not delete any more element
        assert(myMap.size() == 1);
        
        myMap.clear();
    }

}
