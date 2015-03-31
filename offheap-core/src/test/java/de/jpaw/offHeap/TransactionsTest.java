package de.jpaw.offHeap;

import java.util.Arrays;
import org.testng.annotations.Test;


@Test
public class TransactionsTest {
    static public final long KEY = 437L;
    static public final byte [] b1 = "1 One".getBytes();
    static public final byte [] b2 = "2 Two".getBytes();
    static public final byte [] b3 = "3 Three".getBytes();
    static public final byte [] b4 = "4 Four".getBytes();

    private void doAssert(LongToByteArrayOffHeapMap myMap, byte [] what) throws Exception {
        byte [] result = myMap.get(KEY);
        if (!Arrays.equals(result, what))
            throw new Exception("Compare fault: got " + new String(result) + " instead of " + new String(what));
    }

    public void runTxTest() throws Exception {
        OffHeapTransaction tx1 = new OffHeapTransaction(OffHeapTransaction.TRANSACTIONAL);
        Shard s1 = new Shard();
        s1.setOwningTransaction(tx1);

        LongToByteArrayOffHeapMap myMap = new LongToByteArrayOffHeapMap.Builder().setHashSize(1000).setShard(s1).build();
        myMap.set(KEY, b1);
        tx1.setSafepoint();
        myMap.set(KEY, b2);
        doAssert(myMap, b2);
        tx1.rollbackToSafepoint();
        doAssert(myMap, b1);
        tx1.rollback();
        doAssert(myMap, null);

        myMap.close();
        tx1.close();
    }

    public void runTx2Test() throws Exception {
        OffHeapTransaction tx1 = new OffHeapTransaction(OffHeapTransaction.TRANSACTIONAL);
        Shard s1 = new Shard();
        s1.setOwningTransaction(tx1);

        LongToByteArrayOffHeapMap myMap = new LongToByteArrayOffHeapMap.Builder().setHashSize(1000).setShard(s1).build();
        myMap.set(KEY, b1);
        tx1.commit();
        myMap.set(KEY, b2);
        doAssert(myMap, b2);
        tx1.rollback();
        doAssert(myMap, b1);

        myMap.close();
        tx1.close();
    }

    public void runTxClearTest() throws Exception {
        OffHeapTransaction tx1 = new OffHeapTransaction(OffHeapTransaction.TRANSACTIONAL);
        Shard s1 = new Shard();
        s1.setOwningTransaction(tx1);

        LongToByteArrayOffHeapMap myMap = new LongToByteArrayOffHeapMap.Builder().setHashSize(1000).setShard(s1).build();
        myMap.set(KEY, b1);
        tx1.commit();
        myMap.set(KEY, b2);
        myMap.set(KEY+1L, b3);
        assert(myMap.size() == 2);
        doAssert(myMap, b2);
        myMap.clear();
        assert(myMap.size() == 0);
        tx1.printRedoLog();
        tx1.rollback();
        assert(myMap.size() == 1);
        doAssert(myMap, b1);

        myMap.clear();
        tx1.commit();
        assert(myMap.size() == 0);
        tx1.rollback();
        assert(myMap.size() == 0);

        myMap.close();
        tx1.close();
    }

}
