package de.jpaw.offHeap;

import java.util.Arrays;

import org.testng.annotations.Test;


@Test
public class OffHeapMapTest {
    static public final String TEXT = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet";
    static public final Long KEY = 437L;
    
    public void runOpenCloseTest() {
        LongToByteArrayOffHeapMap myMap = new LongToByteArrayOffHeapMap(1000, 99999);
        myMap.close();
    }

    public void runStoreRetrieveTest() {
        LongToByteArrayOffHeapMap myMap = new LongToByteArrayOffHeapMap(1000, 99999);
        byte [] oldData = TEXT.getBytes();
        myMap.set(KEY, oldData);
        
        assert(myMap.get(KEY - 1L) == null);
        assert(myMap.get(KEY + 1L) == null);
        assert(myMap.get(KEY) != null);
        byte [] newData = myMap.get(KEY);
        assert(newData.length == oldData.length);
        String newText = new String(newData);
        System.out.println(TEXT);
        System.out.println(newText);
        assert(Arrays.equals(newData, oldData));
        
        myMap.close();
    }


}
