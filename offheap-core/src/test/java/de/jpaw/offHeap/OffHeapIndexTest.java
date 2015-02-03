package de.jpaw.offHeap;

import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

import de.jpaw.collections.ByteArrayConverter;
import de.jpaw.collections.DuplicateIndexException;
import de.jpaw.collections.InconsistentIndexException;

public class OffHeapIndexTest {
    
    private void insertIndexes(int mode, String ... indexes) {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        PrimitiveLongKeyOffHeapIndex<String> myIndex = new PrimitiveLongKeyOffHeapIndex<String>(
                ByteArrayConverter.STRING_CONVERTER, 1000, mode, "testIdx");
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
        PrimitiveLongKeyOffHeapIndex<String> myIndex = new PrimitiveLongKeyOffHeapIndex<String>(
                ByteArrayConverter.STRING_CONVERTER, 1000, 0x30, "testIdx");
        
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
        PrimitiveLongKeyOffHeapIndex<String> myIndex = new PrimitiveLongKeyOffHeapIndex<String>(
                ByteArrayConverter.STRING_CONVERTER, 1000, 0x30, "testIdx");
        
        myIndex.create(777L, "hello");
        myIndex.delete(777L, "world");
        
        Assert.assertEquals(myIndex.size(), 0);
        
        myIndex.close();
        myMap.close();
    }
    
    @Test
    public void runCreateDeleteTest() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        PrimitiveLongKeyOffHeapIndex<String> myIndex = new PrimitiveLongKeyOffHeapIndex<String>(
                ByteArrayConverter.STRING_CONVERTER, 1000, 0x30, "testIdx");
        
        myIndex.create(777L, "hello");
        myIndex.delete(777L, "hello");
        
        Assert.assertEquals(myIndex.size(), 0);
        
        myIndex.close();
        myMap.close();
    }

    @Test
    public void runCreateReadTest() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        PrimitiveLongKeyOffHeapIndex<String> myIndex = new PrimitiveLongKeyOffHeapIndex<String>(
                ByteArrayConverter.STRING_CONVERTER, 1000, 0x30, "testIdx");
        
        myIndex.create(777L, "hello");
        long result1 = myIndex.getUniqueKeyByIndex("hello");
        Assert.assertNotNull(result1);
        Assert.assertEquals(result1, 777L);
        
        long result2 = myIndex.getUniqueKeyByIndex("world");
        Assert.assertEquals(result2, 0);
        
        myIndex.delete(777L, "hello");
        long result3 = myIndex.getUniqueKeyByIndex("hello");
        Assert.assertEquals(result3, 0);
        
        Assert.assertEquals(myIndex.size(), 0);
        
        myIndex.close();
        myMap.close();
    }
    
    private boolean checkAtLeastOneEntryUsingIterable(PrimitiveLongKeyOffHeapIndex<String> myIndex, String index) {
        Iterable<Long> tmp = myIndex.entriesForIndex(index);
        Iterator<Long> tmp2 = tmp.iterator();
        return tmp2.hasNext();
    }
    
    @Test
    public void runMultiKeyIteratorTest() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        PrimitiveLongKeyOffHeapIndex<String> myIndex = new PrimitiveLongKeyOffHeapIndex<String>(
                ByteArrayConverter.STRING_CONVERTER, 1000, 0x20, "testIdx");
        
        myIndex.create(777L, "hello");
        myIndex.create(778L, "world");
        myIndex.create(779L, "hello");
        
        Assert.assertEquals(myIndex.size(), 3);
        
        int cnt = 0;
        
        Iterable<Long> tmp = myIndex.entriesForIndex("hello");
        System.out.println("got an iterable");
        Iterator<Long> tmp2 = tmp.iterator();
        System.out.println("got an iterator");
        
        System.out.println("hasNext(\"hello\") is " + tmp2.hasNext());
        System.out.println("hasNext(\"none\") is " + checkAtLeastOneEntryUsingIterable(myIndex, "none"));
        System.out.println("hasNext(\"world\") is " + checkAtLeastOneEntryUsingIterable(myIndex, "world"));
        
        for (Long e: myIndex.entriesForIndex("hello")) {
            System.out.println(e);
            ++cnt;
        }
        Assert.assertEquals(cnt, 2);
        
        myIndex.close();
        myMap.close();
    }
    
}
