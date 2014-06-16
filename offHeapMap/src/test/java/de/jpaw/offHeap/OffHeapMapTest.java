package de.jpaw.offHeap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;


@Test
public class OffHeapMapTest {
    static public final String TEXT = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet";
    static public final long KEY = 437L;
    
    public void runOpenCloseTest() {
        LongToByteArrayOffHeapMap myMap = new LongToByteArrayOffHeapMap(1000);
        myMap.close();
    }

    public void runStoreRetrieveTestUncompressed() {
        LongToByteArrayOffHeapMap myMap = new LongToByteArrayOffHeapMap(1000);
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

    public void runStoreRetrieveTestCompressed() {
        LongToByteArrayOffHeapMap myMap = new LongToByteArrayOffHeapMap(1000);
        myMap.setMaxUncompressedSize(0);
        byte [] oldData = TEXT.getBytes();
        myMap.set(KEY, oldData);
        
        assert(myMap.get(KEY - 1L) == null);
        assert(myMap.get(KEY + 1L) == null);
        // myMap.printHistogram(2, null);
        assert(myMap.get(KEY) != null);
        byte [] newData = myMap.get(KEY);
        assert(newData.length == oldData.length);
        String newText = new String(newData);
        System.out.println(TEXT);
        System.out.println(newText);
        assert(Arrays.equals(newData, oldData));
        
        // check lengths
        int len = myMap.length(KEY);
        assert(len == oldData.length);
        
        int compressedLen = myMap.compressedLength(KEY);
        assert(compressedLen < len);  // well, given this text, it should be! This is not valid for any input data, though
        System.out.println("Original length = " + len + ", compressed length = " + compressedLen + ", ratio = " + (100.0 * (float)compressedLen / (float)len));
        
        myMap.close();
    }


    public void runStoreRetrieveTestCompressedLongtext() {
        LongToByteArrayOffHeapMap myMap = new LongToByteArrayOffHeapMap(1000);
        myMap.setMaxUncompressedSize(0);
        byte [] oldData = (TEXT+TEXT+TEXT+TEXT).getBytes();
        myMap.set(KEY, oldData);
        
        byte [] newData = myMap.get(KEY);
        assert(newData != null);
        assert(newData.length == oldData.length);
        assert(Arrays.equals(newData, oldData));
        
        // check lengths
        int len = myMap.length(KEY);
        assert(len == oldData.length);
        
        int compressedLen = myMap.compressedLength(KEY);
        assert(compressedLen < len);  // well, given this text, it should be! This is not valid for any input data, though
        System.out.println("Original length = " + len + ", compressed length = " + compressedLen + ", ratio = " + (100.0 * (float)compressedLen / (float)len));
        
        myMap.close();
    }
    
    public void runStoreRetrieveTestCompressedWithVerify() {
        final int HASH_SIZE = 10000;
        LongToByteArrayOffHeapMap myMap = new LongToByteArrayOffHeapMap(HASH_SIZE);
        myMap.setMaxUncompressedSize(0);
        
        final int MAX_ENTRIES = 3 * HASH_SIZE; 
        final List<byte []> myMem = new ArrayList<byte []>(MAX_ENTRIES);
        
        // pick a size such that we have to have multiple entries per hash map slot
        // compute and store the entries
        for (int i = 0; i < MAX_ENTRIES; ++i) {
            byte [] data = (TEXT + i).getBytes();
            myMem.add(data);
            myMap.set((long)i, data);
        }
        // retrieve and compare them
        for (int i = 0; i < MAX_ENTRIES; ++i) {
            byte [] data = myMap.get((long)i);
            assert(Arrays.equals(data, myMem.get(i)));
        }
        System.out.println("Compare test successful");
        
        // also display the histogram
        myMap.printHistogram(5, null);
        
        myMap.close();
    }

}
