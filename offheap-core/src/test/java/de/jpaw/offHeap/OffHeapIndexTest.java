package de.jpaw.offHeap;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

import de.jpaw.collections.ByteArrayConverter;
import de.jpaw.collections.DuplicateIndexException;
import de.jpaw.collections.InconsistentIndexException;
import de.jpaw.collections.PrimitiveLongKeyMapView;

public class OffHeapIndexTest {
    static public final String TEXT = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet";
    static public final long KEY = 437L;
    static public Charset defCS = StandardCharsets.UTF_8;
    static public final ByteArrayConverter<String> STRING_CONVERTER = new ByteArrayConverter<String>() {

        @Override
        public byte[] valueTypeToByteArray(String arg) {
            return arg == null ? null : arg.getBytes();
        }
        @Override
        public String byteArrayToValueType(byte[] arg) {
            return arg == null ? null : new String(arg);
        }
    };
    
    
    private void insertIndexes(int mode, String ... indexes) {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        PrimitiveLongKeyOffHeapIndex<byte [], String> myIndex = new PrimitiveLongKeyOffHeapIndex<byte [], String>(
                myMap, STRING_CONVERTER, 1000, mode
                );
        long key = 123456789L;
        for (int i = 0; i < indexes.length; ++i)
            myIndex.create(++key, indexes[i]);
        
        Assert.assertEquals(myIndex.size(), indexes.length);
        
        myIndex.close();
        myMap.close();
    }

    @Test
    public void runOpenCloseTest() {
        insertIndexes(0x30);
    }
    
    @Test
    public void runIndexesTest() {
        insertIndexes(0x30, "hello", "world");
    }

    @Test(expectedExceptions = DuplicateIndexException.class)
    public void runIndexesTestWithDupIndexError() {
        insertIndexes(0x30, "hello", "world", "hello");
    }
    
    @Test
    public void runIndexesTestWithDupIndexOk() {
        insertIndexes(0x20, "hello", "world", "hello");
    }
    
    @Test
    public void runCreateUpdateDeleteTest() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        PrimitiveLongKeyOffHeapIndex<byte [], String> myIndex = new PrimitiveLongKeyOffHeapIndex<byte [], String>(
                myMap, STRING_CONVERTER, 1000, 0x30
                );
        
        myIndex.create(777L, "hello");
        myIndex.update(777L, "hello", "world");
        myIndex.delete(777L, "world");
        
        Assert.assertEquals(myIndex.size(), 0);
        
        myIndex.close();
        myMap.close();
    }
    
    
    @Test(expectedExceptions = InconsistentIndexException.class)
    public void runCreateDeleteFailTest() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        PrimitiveLongKeyOffHeapIndex<byte [], String> myIndex = new PrimitiveLongKeyOffHeapIndex<byte [], String>(
                myMap, STRING_CONVERTER, 1000, 0x30
                );
        
        myIndex.create(777L, "hello");
        myIndex.delete(777L, "world");
        
        Assert.assertEquals(myIndex.size(), 0);
        
        myIndex.close();
        myMap.close();
    }
    
    @Test
    public void runCreateDeleteTest() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        PrimitiveLongKeyOffHeapIndex<byte [], String> myIndex = new PrimitiveLongKeyOffHeapIndex<byte [], String>(
                myMap, STRING_CONVERTER, 1000, 0x30
                );
        
        myIndex.create(777L, "hello");
        myIndex.delete(777L, "hello");
        
        Assert.assertEquals(myIndex.size(), 0);
        
        myIndex.close();
        myMap.close();
    }

    @Test
    public void runCreateReadTest() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        PrimitiveLongKeyOffHeapIndex<byte [], String> myIndex = new PrimitiveLongKeyOffHeapIndex<byte [], String>(
                myMap, STRING_CONVERTER, 1000, 0x30
                );
        
        myIndex.create(777L, "hello");
        Long result1 = myIndex.getUniqueKeyByIndex("hello");
        Assert.assertNotNull(result1);
        Assert.assertEquals(result1.longValue(), 777L);
        
        Long result2 = myIndex.getUniqueKeyByIndex("world");
        Assert.assertNull(result2);
        
        myIndex.delete(777L, "hello");
        Long result3 = myIndex.getUniqueKeyByIndex("hello");
        Assert.assertNull(result3);
        
        Assert.assertEquals(myIndex.size(), 0);
        
        myIndex.close();
        myMap.close();
    }
    
    private boolean checkAtLeastOneEntryUsingIterable(PrimitiveLongKeyOffHeapIndex<byte [], String> myIndex, String index) {
        Iterable<PrimitiveLongKeyMapView.Entry<byte[]>> tmp = myIndex.entriesForIndex(index);
        Iterator<PrimitiveLongKeyMapView.Entry<byte[]>> tmp2 = tmp.iterator();
        return tmp2.hasNext();
    }
    
    @Test
    public void runMultiKeyIteratorTest() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        PrimitiveLongKeyOffHeapIndex<byte [], String> myIndex = new PrimitiveLongKeyOffHeapIndex<byte [], String>(
                myMap, STRING_CONVERTER, 1000, 0x20
                );
        
        myIndex.create(777L, "hello");
        myIndex.create(778L, "world");
        myIndex.create(779L, "hello");
        
        Assert.assertEquals(myIndex.size(), 3);
        
        int cnt = 0;
        
        Iterable<PrimitiveLongKeyMapView.Entry<byte[]>> tmp = myIndex.entriesForIndex("hello");
        System.out.println("got an iterable");
        Iterator<PrimitiveLongKeyMapView.Entry<byte[]>> tmp2 = tmp.iterator();
        System.out.println("got an iterator");
        
        System.out.println("hasNext(\"hello\") is " + tmp2.hasNext());
        System.out.println("hasNext(\"none\") is " + checkAtLeastOneEntryUsingIterable(myIndex, "none"));
        System.out.println("hasNext(\"world\") is " + checkAtLeastOneEntryUsingIterable(myIndex, "world"));
        
        for (PrimitiveLongKeyMapView.Entry<byte[]> e: myIndex.entriesForIndex("hello")) {
            System.out.println(e.getKey());
            ++cnt;
        }
        Assert.assertEquals(cnt, 2);
        
        myIndex.close();
        myMap.close();
    }
    
}
