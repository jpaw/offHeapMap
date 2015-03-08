package de.jpaw.offHeap;


import org.testng.Assert;
import org.testng.annotations.Test;

import de.jpaw.collections.ByteArrayConverter;
import de.jpaw.collections.PrimitiveLongIterator;

@Test
public class IndexIteratorTest {
    static public final int NUM = 5000;

    public void runIndexIteratorTest() throws Exception {
        PrimitiveLongKeyOffHeapIndex<String> myIndex = new PrimitiveLongKeyOffHeapIndex<String>(ByteArrayConverter.STRING_CONVERTER, 1000, 0x20, "testIdx");

        // put in some data
        for (int i = 0; i < NUM; ++i) {
            String index = "IND" + (i % 997);
            myIndex.create(i + 3465348956300000L, index);
        }
        Assert.assertEquals(myIndex.size(), NUM);

        int cnt;
        PrimitiveLongIterator myIt;

        // single-call index
        myIt = myIndex.iterator("IND444");
        cnt = 0;
        while (myIt.hasNext()) {
            System.out.println("key is " + myIt.next());
            ++cnt;
        }
        Assert.assertEquals(cnt, 5);

        // batched index
        myIt = myIndex.iterator("IND444", 20, 0);
        cnt = 0;
        while (myIt.hasNext()) {
            System.out.println("key is " + myIt.next());
            ++cnt;
        }
        Assert.assertEquals(cnt, 5);

        // batched index with offset
        System.out.println("Now with skip = 3:");
        myIt = myIndex.iterator("IND444", 3, 3);
        cnt = 0;
        while (myIt.hasNext()) {
            System.out.println("key is " + myIt.next());
            ++cnt;
        }
        Assert.assertEquals(cnt, 2);

        myIndex.close();
    }
}
