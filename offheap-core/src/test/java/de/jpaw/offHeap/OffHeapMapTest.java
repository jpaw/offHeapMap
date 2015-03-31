package de.jpaw.offHeap;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;


@Test
public class OffHeapMapTest {
    static public final String TEXT = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet";
    static public final long KEY = 437L;
    static public Charset defCS = StandardCharsets.UTF_8;

    public void runOpenCloseTest() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        myMap.close();
    }

    public void runStoreRetrieveTestUncompressed() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
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
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
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
        System.out.println("Original length = " + len + ", compressed length = " + compressedLen + ", ratio = " + (100.0 * compressedLen / len));

        myMap.close();
    }


    public void runStoreRetrieveTestCompressedLongtext() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
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
        System.out.println("Original length = " + len + ", compressed length = " + compressedLen + ", ratio = " + (100.0 * compressedLen / len));

        myMap.close();
    }

    public void runStoreRetrieveTestCompressedWithVerify() {
        final int HASH_SIZE = 10000;
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(HASH_SIZE);
        myMap.setMaxUncompressedSize(0);

        final int MAX_ENTRIES = 3 * HASH_SIZE;
        final List<byte []> myMem = new ArrayList<byte []>(MAX_ENTRIES);

        // pick a size such that we have to have multiple entries per hash map slot
        // compute and store the entries
        for (int i = 0; i < MAX_ENTRIES; ++i) {
            byte [] data = (TEXT + i).getBytes();
            myMem.add(data);
            myMap.set(i, data);
        }
        // retrieve and compare them
        for (int i = 0; i < MAX_ENTRIES; ++i) {
            byte [] data = myMap.get(i);
            assert(Arrays.equals(data, myMem.get(i)));
        }
        System.out.println("Compare test successful");

        // also display the histogram
        myMap.printHistogram(5, null);

        myMap.close();
    }

    public void storeIntoBufferTransfer() {
        int len;
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(20);
        myMap.set(KEY,  "4444".getBytes(defCS));
        byte [] buffer = "xxxxxxxxxx".getBytes(defCS);

        len = myMap.getIntoBuffer(KEY, buffer, 3);
        System.out.println("Buffer is " + new String(buffer, defCS));
        assert(Arrays.equals(buffer, "xxx4444xxx".getBytes(defCS)));
        assert(len == 4);

        len = myMap.getIntoBuffer(KEY, buffer, 8);
        System.out.println("Buffer is " + new String(buffer, defCS));
        assert(Arrays.equals(buffer, "xxx4444x44".getBytes(defCS)));
        assert(len == 2);

        myMap.close();

    }

    public void getField() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(20);
        myMap.set(KEY,  ";hello;world;;field4;@@field7;end".getBytes(defCS));
        String [] expected = { "", "hello", "world", "", "field4", null, null, "field7", "end", null, null };

        for (int i = 0; i <= 10; ++i) {
            byte[] word = myMap.getField(KEY, i, (byte)';', (byte)'@');
            System.out.println("Word " + i + " is " + (word == null ? "NULL" : new String(word, defCS)));

            if (expected[i] == null)
                assert(word == null);
            else
                assert(word != null && expected[i].equals(new String(word, defCS)));
        }

        myMap.close();
    }

    public void getRegionTest() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(20);
        byte [] data = "John likes to play foo bar with his dog".getBytes(defCS);
        myMap.set(KEY, data);

        // test inner part
        byte [] playFooBar = myMap.getRegion(KEY, 14, 12);
        assert(playFooBar != null);
        assert(new String(playFooBar, defCS).equals("play foo bar"));

        // test exceeding end of object
        byte [] withHisDogAndMore = myMap.getRegion(KEY, 27, 50);
        assert(withHisDogAndMore != null);
        assert(new String(withHisDogAndMore, defCS).equals("with his dog"));


        myMap.setFromBuffer(KEY+1L, data, 14, 12);
        byte [] playFooBar2 = myMap.get(KEY+1L);
        assert(playFooBar2 != null);
        assert(new String(playFooBar2, defCS).equals("play foo bar"));

        // test exceeding end of object: throws illegal argument exception (caught by Java)
//        myMap.setFromBuffer(KEY+2L, data, 27, 50);
//        byte [] withHisDogAndMore2 = myMap.get(KEY+2L);
//        assert(withHisDogAndMore2 != null);
//        assert(new String(withHisDogAndMore2, defCS).equals("with his dog"));

        myMap.close();
    }
}
