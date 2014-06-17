package de.jpaw.offHeap;

import org.testng.annotations.Test;

@Test
public class StringMapTest {
    static public final String TEXT = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet";
    static public final long KEY = 437L;
    
    public void runOpenCloseTest() {
        LongToStringOffHeapMap myMap = new LongToStringOffHeapMap(1000);
        myMap.close();
    }

    public void runStoreRetrieveTestUncompressed() {
        LongToStringOffHeapMap myMap = new LongToStringOffHeapMap(1000);
        
        myMap.set(KEY, TEXT);
        
        assert(myMap.get(KEY - 1L) == null);
        assert(myMap.get(KEY + 1L) == null);
        assert(myMap.get(KEY) != null);
        String newString = myMap.get(KEY);
        assert(TEXT.equals(newString));
        
        myMap.set(1L, "veni");
        myMap.set(2L, "vidi");
        myMap.set(3L, "vici");

        myMap.forEach(kvp -> System.out.println(String.format("Element is %d,%s", kvp.getKey(), kvp.getValue())));
        
        myMap.writeToFile("/tmp/testDB.ohm");
        myMap.close();
    }


}
