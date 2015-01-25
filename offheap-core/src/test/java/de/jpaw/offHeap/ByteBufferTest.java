package de.jpaw.offHeap;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ByteBufferTest {
    static public final String TEXT = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet";
    static public final long KEY = 437L;
    static public Charset defCS = StandardCharsets.UTF_8;
    
    @Test
    public void runStoreRetrieveTestUncompressed() {
        LongToByteArrayOffHeapMap myMap = LongToByteArrayOffHeapMap.forHashSize(1000);
        byte [] oldData = TEXT.getBytes();
        myMap.set(KEY, oldData);
        
        ByteBuffer result = myMap.getAsByteBuffer(KEY);
        
        Assert.assertNotNull(result);
        
        System.out.println(
                "Is direct: " + result.isDirect()
                + "\nIs readonly: " + result.isReadOnly()
                + "\ncapacity: " + result.capacity()
                + "\nposition: " + result.position()
                + "\nlimit: " + result.position());
        
        Assert.assertEquals(result.isDirect(), true);
        Assert.assertEquals(result.isReadOnly(), false);
        Assert.assertEquals(result.capacity(), TEXT.length());
        Assert.assertEquals(result.limit(), TEXT.length());     // subject to ASCII only characters in the sample string!
        Assert.assertEquals(result.position(), 0);
        myMap.close();
    }
}
