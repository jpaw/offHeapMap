package de.jpaw.offHeap;

import org.testng.annotations.Test;

import de.jpaw.collections.PrimitiveLongKeyMapView;

@Test
public class ViewIndexTransactionTest {
    public static final String DATAVALUE = "seventeen";
    public static final String INDEXVALUE = "i4-17";
    
    // This test demonstrates that the additional view stays unmodified unless a commit() is done, and then the view matches the map again
    public void runViewIndexTest() throws Exception {
        OffHeapTransaction tx1 = new OffHeapTransaction(OffHeapTransaction.TRANSACTIONAL);
        Shard s1 = new Shard();
        s1.setOwningTransaction(tx1);
    
        LongToStringOffHeapMap myMap = new LongToStringOffHeapMap.Builder()
            .setHashSize(1000)
            .setShard(s1)
            .addCommittedView()
            .build();
        PrimitiveLongKeyMapView<String> myView = myMap.getView();
        
        // index
        PrimitiveLongKeyOffHeapIndex<String, String> myIndex = new PrimitiveLongKeyOffHeapIndex<String, String>(
                myMap, OffHeapIndexTest.STRING_CONVERTER, 1000, 0x31  // transactional unique view
                );
        PrimitiveLongKeyOffHeapIndexView<String,String> myIndexView = myIndex.getView();
        
        myMap.set(1L, "The");
        myMap.set(2L, "next");
        myMap.set(3L, "is");
        myMap.set(17L, DATAVALUE);
        myIndex.create(17L, INDEXVALUE);
        
        assert(myMap.size() == 4);
        assert(myView.size() == 0);
        
        assert(myIndex.size() == 1);
        assert(myIndexView.size() == 0);
        
        tx1.commit();
        assert(myMap.size() == 4);
        assert(myView.size() == 4);

        assert(myIndex.size() == 1);
        assert(myIndexView.size() == 1);

        assert(DATAVALUE.equals(myView.get(17L)));
        assert(Long.valueOf(17L).equals(myIndexView.getUniqueKeyByIndex(INDEXVALUE)));
        
        myMap.remove(17L);
        myMap.set(3L, "was");
        myMap.set(80L, "eighty");
        
        myIndex.delete(17L, INDEXVALUE);

        assert(myMap.size() == 4);
        assert(myView.size() == 4);
        assert(DATAVALUE.equals(myView.get(17L)));
        assert(myView.get(80L) == null);

        assert(myIndex.size() == 0);
        assert(myIndexView.size() == 1);

        // can still read the index on the committed view...
        assert(Long.valueOf(17L).equals(myIndexView.getUniqueKeyByIndex(INDEXVALUE)));
        
        tx1.commit();
        
        assert(myView.size() == 4);
        assert(myView.get(80L) != null);
        
        assert(myIndex.size() == 0);
        assert(myIndexView.size() == 0);

        assert(myIndexView.getUniqueKeyByIndex(INDEXVALUE) == null);  // but now it should be deleted

        tx1.close();
        myMap.close();
    }

}
