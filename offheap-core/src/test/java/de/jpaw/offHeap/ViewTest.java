package de.jpaw.offHeap;

import org.testng.annotations.Test;

import de.jpaw.collections.PrimitiveLongKeyMapView;

@Test
public class ViewTest {

    // This test demonstrates that the additional view stays unmodified unless a commit() is done, and then the view matches the map again
    public void runViewTest() throws Exception {
        OffHeapTransaction tx1 = new OffHeapTransaction(OffHeapTransaction.TRANSACTIONAL);
        Shard s1 = new Shard();
        s1.setOwningTransaction(tx1);

        LongToStringOffHeapMap myMap = new LongToStringOffHeapMap.Builder()
            .setHashSize(1000)
            .setShard(s1)
            .addCommittedView()
            .build();
        PrimitiveLongKeyMapView<String> myView = myMap.getView();

        myMap.set(1L, "The");
        myMap.set(2L, "next");
        myMap.set(3L, "is");
        myMap.set(17L, "seventeen");

        assert(myMap.size() == 4);
        assert(myView.size() == 0);

        tx1.commit();
        assert(myMap.size() == 4);
        assert(myView.size() == 4);

        assert("seventeen".equals(myView.get(17L)));

        myMap.remove(17L);
        myMap.set(3L, "was");
        myMap.set(80L, "eighty");

        assert(myMap.size() == 4);
        assert(myView.size() == 4);
        assert("seventeen".equals(myView.get(17L)));
        assert(myView.get(80L) == null);

        tx1.commit();
        assert(myView.size() == 4);
        assert(myView.get(80L) != null);

        tx1.close();
        myMap.close();
    }

    // This test demonstrates that the additional view stays unmodified unless a commit() is done, and then the view matches the map again
    public void runDelayedUpdViewTest() throws Exception {
        OffHeapTransaction tx1 = new OffHeapTransaction(OffHeapTransaction.TRANSACTIONAL);
        Shard s1 = new Shard();
        s1.setOwningTransaction(tx1);

        LongToStringOffHeapMap myMap = new LongToStringOffHeapMap.Builder()
            .setHashSize(1000)
            .setShard(s1)
            .addCommittedView()
            .build();
        PrimitiveLongKeyMapView<String> myView = myMap.getView();

        myMap.set(1L, "The");
        myMap.set(2L, "next");
        myMap.set(3L, "is");
        myMap.set(17L, "seventeen");

        assert(myMap.size() == 4);
        assert(myView.size() == 0);

        long myDelayedTx1 = tx1.commitDelayedUpdate();
        assert(myMap.size() == 4);
        assert(myView.size() == 0);   // not yet updated

        myMap.remove(17L);
        myMap.set(3L, "was");
        myMap.set(80L, "eighty");
        myMap.delete(2L);

        assert(myMap.size() == 3);

        long myDelayedTx2 = tx1.commitDelayedUpdate();

        assert(myView.size() == 0);   // not yet updated

        myMap.clear();

        tx1.updateViews(myDelayedTx1);

        assert("seventeen".equals(myView.get(17L)));

        assert(myView.size() == 4);

        assert(myView.get(80L) == null);
        tx1.updateViews(myDelayedTx2);
        assert(myView.get(80L) != null);

        assert(myView.size() == 3);
        assert(myMap.size() == 0);
        tx1.commit();
        assert(myMap.size() == 0);
        assert(myView.size() == 0);

        tx1.close();
        myMap.close();
    }

}
