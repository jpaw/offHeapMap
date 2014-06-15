package de.jpaw.jni.bench;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.logic.BlackHole;

import de.jpaw.offHeap.LongToByteArrayOffHeapMap;
import de.jpaw.offHeap.OffHeapTransaction;
import de.jpaw.offHeap.Shard;

// Invocation:
// java -Djava.library.path=$HOME/lib -jar target/offHeapBench.jar -i 3 -f 3 -wf 1 -wi 3
//
// output on an Intel i7 980
//
//Benchmark                                        Mode   Samples         Mean   Mean error    Units
//d.j.j.b.OffHeapMapBench.commitOpNoRows           avgt         9       13.347        0.528    ns/op
//d.j.j.b.OffHeapMapBench.insertCommitGetOp        avgt         9      134.339        6.545    ns/op
//d.j.j.b.OffHeapMapBench.insertCommitOp           avgt         9      113.558        4.083    ns/op
//d.j.j.b.OffHeapMapBench.insertGetOpNoTx          avgt         9      119.524        6.828    ns/op
//d.j.j.b.OffHeapMapBench.insertOpNoTx             avgt         9       98.867        2.360    ns/op
//d.j.j.b.OffHeapMapBench.insertRemoveCommitOp     avgt         9      132.018        5.663    ns/op
//d.j.j.b.OffHeapMapBench.insertRemoveOpNoTx       avgt         9      122.806       10.912    ns/op
//d.j.j.b.OffHeapMapBench.removeOpNoTx             avgt         9       17.074        0.912    ns/op
//d.j.j.b.OffHeapMapBench.removeOpWithTx           avgt         9       30.245        1.029    ns/op
//d.j.j.b.OffHeapMapBench.sizeOpNoTx               avgt         9       11.608        1.355    ns/op
//d.j.j.b.OffHeapMapBench.sizeOpWithTx             avgt         9       12.087        1.318    ns/op

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(value = Scope.Thread)
public class OffHeapMapBench {
    private static final long KEY = 552528219827L;
    private static final long CHANGE_ID = 553428219827L;
    private static final byte [] SHORTDATA = { (byte)1, (byte)2, (byte)3 };
    
    private LongToByteArrayOffHeapMap mapNoTransactions = null;
    private LongToByteArrayOffHeapMap mapWithTransactions = null;
    private Shard defaultShard = new Shard();
    private OffHeapTransaction transaction = null;
    
    @Setup
    public void setUp() {
        mapNoTransactions = new LongToByteArrayOffHeapMap(1000);
        mapWithTransactions = new LongToByteArrayOffHeapMap(1000, defaultShard);
        transaction = new OffHeapTransaction(OffHeapTransaction.TRANSACTIONAL);
        defaultShard.setOwningTransaction(transaction);
    }

    @TearDown
    public void tearDown() {
        mapNoTransactions.close();
        transaction.commit();  // just in case...
        mapWithTransactions.close();
        transaction.close();
    }
    
    @GenerateMicroBenchmark
    public void sizeOpNoTx(BlackHole bh) {
        bh.consume(mapNoTransactions.size());
    }

    @GenerateMicroBenchmark
    public void sizeOpWithTx(BlackHole bh) {
        bh.consume(mapWithTransactions.size());
    }

    @GenerateMicroBenchmark
    public void commitOpNoRows() {
        transaction.commit(CHANGE_ID);
    }

    @GenerateMicroBenchmark
    public void insertOpNoTx() {
        mapNoTransactions.set(KEY, SHORTDATA);
    }

    @GenerateMicroBenchmark
    public void insertGetOpNoTx(BlackHole bh) {
        mapNoTransactions.set(KEY, SHORTDATA);
        bh.consume(mapNoTransactions.get(KEY));
    }

    @GenerateMicroBenchmark
    public void insertRemoveOpNoTx() {
        mapNoTransactions.set(KEY, SHORTDATA);
        mapNoTransactions.remove(KEY);
    }
    
    @GenerateMicroBenchmark
    public void insertCommitOp() {
        mapWithTransactions.set(KEY, SHORTDATA);
        transaction.commit(CHANGE_ID);
    }
    
    @GenerateMicroBenchmark
    public void insertCommitGetOp(BlackHole bh) {
        mapWithTransactions.set(KEY, SHORTDATA);
        transaction.commit(CHANGE_ID);
        bh.consume(mapWithTransactions.get(KEY));
    }

    @GenerateMicroBenchmark
    public void insertRemoveCommitOp() {
        mapWithTransactions.set(KEY, SHORTDATA);
        mapWithTransactions.remove(KEY);
        transaction.commit(CHANGE_ID);
    }
    
    @GenerateMicroBenchmark
    public void removeOpNoTx() {
        mapNoTransactions.remove(KEY);
    }
    
    @GenerateMicroBenchmark
    public void removeOpWithTx() {
        mapWithTransactions.remove(KEY);
        transaction.commit(CHANGE_ID);
    }

}
