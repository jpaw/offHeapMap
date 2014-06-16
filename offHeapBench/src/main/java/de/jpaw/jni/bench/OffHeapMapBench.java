package de.jpaw.jni.bench;

import java.io.UnsupportedEncodingException;
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
//d.j.j.b.OffHeapMapBench.insertDeleteCommitOp     avgt         9      132.018        5.663    ns/op
//d.j.j.b.OffHeapMapBench.insertDeleteOpNoTx       avgt         9      122.806       10.912    ns/op
//d.j.j.b.OffHeapMapBench.deleteOpNoTx             avgt         9       17.074        0.912    ns/op
//d.j.j.b.OffHeapMapBench.deleteOpWithTx           avgt         9       30.245        1.029    ns/op
//

// compare size when using ((*env)->GetLongField(env, me, javaMapCStructFID)) vs passing it as a parameter:
//d.j.j.b.OffHeapMapBench.sizeOpNoTx               avgt         9       11.608        1.355    ns/op  // GetLongField
//d.j.j.b.OffHeapMapBench.sizeOpWithTx             avgt         9       12.087        1.318    ns/op  // GetLongField
//d.j.j.b.OffHeapMapBench.sizeOpNoTx               avgt        15        8.833        0.196    ns/op  // parameter
//d.j.j.b.OffHeapMapBench.sizeOpWithTx             avgt        15        8.400        0.268    ns/op  // parameter
// => This is consistent with http://www.ibm.com/developerworks/library/j-jni/, passign as a parameter is 3 ns faster per invocation

//Benchmark                                               Mode   Samples         Mean   Mean error    Units
//d.j.j.b.OffHeapMapBench.getSmallCompressedOp            avgt         9       20.295        0.918    ns/op
//d.j.j.b.OffHeapMapBench.getSmallOp                      avgt         9       20.317        0.450    ns/op
//d.j.j.b.OffHeapMapBench.insertCompressedSmallOpNoTx     avgt         9      447.336        2.338    ns/op
//d.j.j.b.OffHeapMapBench.insertSmallOpNoTx               avgt         9       98.523        0.891    ns/op
//
//d.j.j.b.OffHeapMapBench.get1KBCompressedOp              avgt         9       20.214        1.779    ns/op
//d.j.j.b.OffHeapMapBench.get1KBOp                        avgt         9       19.881        0.498    ns/op
//d.j.j.b.OffHeapMapBench.insert1KBOpNoTx                 avgt         9      163.259       15.927    ns/op
//d.j.j.b.OffHeapMapBench.insertCompressed1KBOpNoTx       avgt         9     1184.993       19.434    ns/op
//
// => compression speed is about 700 ns / KB => 1.3 GB / s

// everything measured with new parameter passing: all got faster except read, which got much worse
//Benchmark                                               Mode   Samples         Mean   Mean error    Units
//d.j.j.b.OffHeapMapBench.commitOpNoRows                  avgt        15       13.444        0.540    ns/op
//d.j.j.b.OffHeapMapBench.deleteOpNoTx                    avgt        15       14.409        1.118    ns/op
//d.j.j.b.OffHeapMapBench.deleteOpWithTx                  avgt        15       28.370        1.117    ns/op
//d.j.j.b.OffHeapMapBench.get1KBCompressedOp              avgt        15      310.284        8.393    ns/op
//d.j.j.b.OffHeapMapBench.get1KBOp                        avgt        15      178.899        4.772    ns/op
//d.j.j.b.OffHeapMapBench.getSmallCompressedOp            avgt        15      102.874        1.119    ns/op
//d.j.j.b.OffHeapMapBench.getSmallOp                      avgt        15       86.800        2.011    ns/op
//d.j.j.b.OffHeapMapBench.insert1KBOpNoTx                 avgt        15      140.631        8.430    ns/op
//d.j.j.b.OffHeapMapBench.insertCommitGetOp               avgt        15      191.439        5.975    ns/op
//d.j.j.b.OffHeapMapBench.insertCommitOp                  avgt        15      100.207        1.412    ns/op
//d.j.j.b.OffHeapMapBench.insertCompressed1KBOpNoTx       avgt        15     1184.775       74.685    ns/op
//d.j.j.b.OffHeapMapBench.insertCompressedSmallOpNoTx     avgt        15      442.646        6.547    ns/op
//d.j.j.b.OffHeapMapBench.insertDeleteCommitOp            avgt        15      114.781        1.299    ns/op
//d.j.j.b.OffHeapMapBench.insertDeleteOpNoTx              avgt        15      103.353        2.651    ns/op
//d.j.j.b.OffHeapMapBench.insertGetOpNoTx                 avgt        15      182.117       13.877    ns/op
//d.j.j.b.OffHeapMapBench.insertSmallOpNoTx               avgt        15       92.397        3.413    ns/op
//d.j.j.b.OffHeapMapBench.sizeOpNoTx                      avgt        15        8.549        0.443    ns/op
//d.j.j.b.OffHeapMapBench.sizeOpWithTx                    avgt        15        9.761        1.256    ns/op

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(value = Scope.Thread)
public class OffHeapMapBench {
    static public final String TEXT = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet";
    private static final long KEY = 552528219827L;
    private static final long KEY_PERM_SMALL = 552528219828L;
    private static final long KEY_PERM_1KB = 552528219829L;
    private static final long CHANGE_ID = 553428219827L;
    private static final byte [] SHORTDATA = { (byte)1, (byte)2, (byte)3 };
    
    private LongToByteArrayOffHeapMap mapNoTransactions = null;
    private LongToByteArrayOffHeapMap mapNoTransactionsComp = null;
    private LongToByteArrayOffHeapMap mapWithTransactions = null;
    private Shard defaultShard = new Shard();
    private OffHeapTransaction transaction = null;
    private byte [] data1KB = null;
    
    @Setup
    public void setUp() throws UnsupportedEncodingException {
        mapNoTransactions = new LongToByteArrayOffHeapMap(1000);
        mapNoTransactionsComp = new LongToByteArrayOffHeapMap(1000);
        mapNoTransactionsComp.setMaxUncompressedSize(1);
        mapWithTransactions = new LongToByteArrayOffHeapMap(1000, defaultShard);
        transaction = new OffHeapTransaction(OffHeapTransaction.TRANSACTIONAL);
        defaultShard.setOwningTransaction(transaction);
        data1KB = (TEXT + TEXT + TEXT + TEXT).substring(0, 1024).getBytes("UTF-8");
        
        mapNoTransactions.set(KEY_PERM_SMALL, SHORTDATA);
        mapNoTransactions.set(KEY_PERM_1KB, data1KB);
        mapNoTransactionsComp.set(KEY_PERM_SMALL, SHORTDATA);
        mapNoTransactionsComp.set(KEY_PERM_1KB, data1KB);
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
    public void insertSmallOpNoTx() {
        mapNoTransactions.set(KEY, SHORTDATA);
    }

    @GenerateMicroBenchmark
    public void insert1KBOpNoTx() {
        mapNoTransactions.set(KEY, data1KB);
    }

    @GenerateMicroBenchmark
    public void insertCompressedSmallOpNoTx() {
        mapNoTransactionsComp.set(KEY, SHORTDATA);
    }

    @GenerateMicroBenchmark
    public void insertCompressed1KBOpNoTx() {
        mapNoTransactionsComp.set(KEY, data1KB);
    }


    
    @GenerateMicroBenchmark
    public void getSmallOp(BlackHole bh) {
        bh.consume(mapNoTransactions.get(KEY_PERM_SMALL));
    }
    
    @GenerateMicroBenchmark
    public void get1KBOp(BlackHole bh) {
        bh.consume(mapNoTransactions.get(KEY_PERM_1KB));
    }
    
    @GenerateMicroBenchmark
    public void getSmallCompressedOp(BlackHole bh) {
        bh.consume(mapNoTransactionsComp.get(KEY_PERM_SMALL));
    }
    
    @GenerateMicroBenchmark
    public void get1KBCompressedOp(BlackHole bh) {
        bh.consume(mapNoTransactionsComp.get(KEY_PERM_1KB));
    }


    
    @GenerateMicroBenchmark
    public void insertGetOpNoTx(BlackHole bh) {
        mapNoTransactions.set(KEY, SHORTDATA);
        bh.consume(mapNoTransactions.get(KEY));
    }

    @GenerateMicroBenchmark
    public void insertDeleteOpNoTx() {
        mapNoTransactions.set(KEY, SHORTDATA);
        mapNoTransactions.delete(KEY);
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
    public void insertDeleteCommitOp() {
        mapWithTransactions.set(KEY, SHORTDATA);
        mapWithTransactions.delete(KEY);
        transaction.commit(CHANGE_ID);
    }
    
    @GenerateMicroBenchmark
    public void deleteOpNoTx() {
        mapNoTransactions.delete(KEY);
    }
    
    @GenerateMicroBenchmark
    public void deleteOpWithTx() {
        mapWithTransactions.delete(KEY);
        transaction.commit(CHANGE_ID);
    }

}
