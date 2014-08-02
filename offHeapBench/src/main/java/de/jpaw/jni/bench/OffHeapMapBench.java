package de.jpaw.jni.bench;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import de.jpaw.offHeap.LongToByteArrayOffHeapMap;
import de.jpaw.offHeap.OffHeapTransaction;
import de.jpaw.offHeap.Shard;

// Invocation:
// java -Djava.library.path=$HOME/lib -jar target/offHeapBench.jar -i 3 -f 3 -wf 1 -wi 3
//
// output on an Intel i7 980
//
// compare size when using ((*env)->GetLongField(env, me, javaMapCStructFID)) vs passing it as a parameter:
//d.j.j.b.OffHeapMapBench.sizeOpNoTx               avgt         9       11.608        1.355    ns/op  // GetLongField
//d.j.j.b.OffHeapMapBench.sizeOpWithTx             avgt         9       12.087        1.318    ns/op  // GetLongField
//d.j.j.b.OffHeapMapBench.sizeOpNoTx               avgt        15        8.833        0.196    ns/op  // parameter
//d.j.j.b.OffHeapMapBench.sizeOpWithTx             avgt        15        8.400        0.268    ns/op  // parameter
// => This is consistent with http://www.ibm.com/developerworks/library/j-jni/, passign as a parameter is 3 ns faster per invocation

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

// Benchmark after refactoring to abstract parent class:
//Benchmark                                               Mode   Samples         Mean   Mean error    Units
//d.j.j.b.OffHeapMapBench.commitOpNoRows                  avgt         6       10.891        0.287    ns/op
//d.j.j.b.OffHeapMapBench.deleteOpNoTx                    avgt         6       13.731        0.286    ns/op
//d.j.j.b.OffHeapMapBench.deleteOpWithTx                  avgt         6       24.758        0.128    ns/op
//d.j.j.b.OffHeapMapBench.get1KBCompressedOp              avgt         6      383.262       16.040    ns/op
//d.j.j.b.OffHeapMapBench.get1KBOp                        avgt         6      186.303       30.967    ns/op
//d.j.j.b.OffHeapMapBench.getSmallCompressedOp            avgt         6      128.687        6.026    ns/op
//d.j.j.b.OffHeapMapBench.getSmallOp                      avgt         6       86.294        4.179    ns/op
//d.j.j.b.OffHeapMapBench.insert1KBOpNoTx                 avgt         6      145.011       22.454    ns/op
//d.j.j.b.OffHeapMapBench.insertCommitGetOp               avgt         6      204.663       10.392    ns/op
//d.j.j.b.OffHeapMapBench.insertCommitOp                  avgt         6      101.231        3.265    ns/op
//d.j.j.b.OffHeapMapBench.insertCompressed1KBOpNoTx       avgt         6     1170.021       60.519    ns/op
//d.j.j.b.OffHeapMapBench.insertCompressedSmallOpNoTx     avgt         6      441.709       13.129    ns/op
//d.j.j.b.OffHeapMapBench.insertDeleteCommitOp            avgt         6      115.040        5.159    ns/op
//d.j.j.b.OffHeapMapBench.insertDeleteOpNoTx              avgt         6      102.764        4.071    ns/op
//d.j.j.b.OffHeapMapBench.insertGetOpNoTx                 avgt         6      177.548        1.726    ns/op
//d.j.j.b.OffHeapMapBench.insertSmallOpNoTx               avgt         6       88.208        0.450    ns/op
//d.j.j.b.OffHeapMapBench.sizeOpNoTx                      avgt         6        8.756        0.075    ns/op
//d.j.j.b.OffHeapMapBench.sizeOpWithTx                    avgt         6        8.400        1.050    ns/op

// after introduction of committed view: commit & insert got slower, but acceptable overhead
//Benchmark                                               Mode   Samples         Mean   Mean error    Units
//d.j.j.b.OffHeapMapBench.commitOpNoRows                  avgt        12       11.235        0.033    ns/op
//d.j.j.b.OffHeapMapBench.deleteOpNoTx                    avgt        12       13.656        0.030    ns/op
//d.j.j.b.OffHeapMapBench.deleteOpWithTx                  avgt        12       25.557        0.381    ns/op
//d.j.j.b.OffHeapMapBench.get1KBCompressedOp              avgt        12      381.435        9.438    ns/op
//d.j.j.b.OffHeapMapBench.get1KBOp                        avgt        12      177.117        2.127    ns/op
//d.j.j.b.OffHeapMapBench.getSmallCompressedOp            avgt        12      125.873        6.484    ns/op
//d.j.j.b.OffHeapMapBench.getSmallOp                      avgt        12       88.388        3.385    ns/op
//d.j.j.b.OffHeapMapBench.insert1KBOpNoTx                 avgt        12      134.848        1.351    ns/op
//d.j.j.b.OffHeapMapBench.insertCommitGetOp               avgt        12      193.674        5.197    ns/op
//d.j.j.b.OffHeapMapBench.insertCommitOp                  avgt        12      100.884        1.903    ns/op
//d.j.j.b.OffHeapMapBench.insertCompressed1KBOpNoTx       avgt        12     1196.368       24.133    ns/op
//d.j.j.b.OffHeapMapBench.insertCompressedSmallOpNoTx     avgt        12      468.316       37.412    ns/op
//d.j.j.b.OffHeapMapBench.insertDeleteCommitOp            avgt        12      117.903        4.365    ns/op
//d.j.j.b.OffHeapMapBench.insertDeleteOpNoTx              avgt        12      105.900        4.659    ns/op
//d.j.j.b.OffHeapMapBench.insertGetOpNoTx                 avgt        12      180.652        3.838    ns/op
//d.j.j.b.OffHeapMapBench.insertSmallOpNoTx               avgt        12       88.064        0.936    ns/op
//d.j.j.b.OffHeapMapBench.sizeOpNoTx                      avgt        12        8.534        0.367    ns/op
//d.j.j.b.OffHeapMapBench.sizeOpWithTx                    avgt        12        8.309        0.410    ns/op

// after introduction of Builder pattern and view Java subclass (& valueConverter in component instead of main class)
//Benchmark                                               Mode   Samples         Mean   Mean error    Units
//d.j.j.b.OffHeapMapBench.commitOpNoRows                  avgt        12       11.317        0.291    ns/op
//d.j.j.b.OffHeapMapBench.deleteOpNoTx                    avgt        12       13.657        0.030    ns/op
//d.j.j.b.OffHeapMapBench.deleteOpWithTx                  avgt        12       25.449        0.351    ns/op
//d.j.j.b.OffHeapMapBench.get1KBCompressedOp              avgt        12      374.109        6.314    ns/op
//d.j.j.b.OffHeapMapBench.get1KBOp                        avgt        12      178.464        5.584    ns/op
//d.j.j.b.OffHeapMapBench.getSmallCompressedOp            avgt        12      127.678        2.225    ns/op
//d.j.j.b.OffHeapMapBench.getSmallOp                      avgt        12       88.902        1.524    ns/op
//d.j.j.b.OffHeapMapBench.insert1KBOpNoTx                 avgt        12      130.551        4.046    ns/op
//d.j.j.b.OffHeapMapBench.insertCommitGetOp               avgt        12      208.478       10.022    ns/op
//d.j.j.b.OffHeapMapBench.insertCommitOp                  avgt        12      111.114        3.710    ns/op
//d.j.j.b.OffHeapMapBench.insertCompressed1KBOpNoTx       avgt        12     1169.077       18.414    ns/op
//d.j.j.b.OffHeapMapBench.insertCompressedSmallOpNoTx     avgt        12      449.142       27.643    ns/op
//d.j.j.b.OffHeapMapBench.insertDeleteCommitOp            avgt        12      164.773       47.153    ns/op
//d.j.j.b.OffHeapMapBench.insertDeleteOpNoTx              avgt        12      103.886        3.076    ns/op
//d.j.j.b.OffHeapMapBench.insertGetOpNoTx                 avgt        12      180.565        6.324    ns/op
//d.j.j.b.OffHeapMapBench.insertSmallOpNoTx               avgt        12       88.059        1.986    ns/op
//d.j.j.b.OffHeapMapBench.sizeOpNoTx                      avgt        12        8.721        0.017    ns/op
//d.j.j.b.OffHeapMapBench.sizeOpWithTx                    avgt        12        8.565        0.430    ns/op
// remeasure insertDeleteCommitOp:
//d.j.j.b.OffHeapMapBench.insertDeleteCommitOp            avgt        12      141.021        7.319    ns/op

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(value = Scope.Thread)
public class OffHeapMapBench {
    static public final String TEXT = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet";
    private static final long KEY = 552528219827L;
    private static final long KEY_PERM_SMALL = 552528219828L;
    private static final long KEY_PERM_1KB = 552528219829L;
    private static final byte [] SHORTDATA = { (byte)1, (byte)2, (byte)3 };
    
    private LongToByteArrayOffHeapMap mapNoTransactions = null;
    private LongToByteArrayOffHeapMap mapNoTransactionsComp = null;
    private LongToByteArrayOffHeapMap mapWithTransactions = null;
    private Shard defaultShard = new Shard();
    private OffHeapTransaction transaction = null;
    private byte [] data1KB = null;
    
    @Setup
    public void setUp() throws UnsupportedEncodingException {
        mapNoTransactions = LongToByteArrayOffHeapMap.forHashSize(1000);
        mapNoTransactionsComp = LongToByteArrayOffHeapMap.forHashSize(1000);
        mapNoTransactionsComp.setMaxUncompressedSize(1);
        mapWithTransactions = new LongToByteArrayOffHeapMap.Builder().setHashSize(1000).setShard(defaultShard).build();
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
    
    @Benchmark
    public void sizeOpNoTx(Blackhole bh) {
        bh.consume(mapNoTransactions.size());
    }

    @Benchmark
    public void sizeOpWithTx(Blackhole bh) {
        bh.consume(mapWithTransactions.size());
    }

    @Benchmark
    public void commitOpNoRows() {
        transaction.commit();
    }

    
    
    @Benchmark
    public void insertSmallOpNoTx() {
        mapNoTransactions.set(KEY, SHORTDATA);
    }

    @Benchmark
    public void insert1KBOpNoTx() {
        mapNoTransactions.set(KEY, data1KB);
    }

    @Benchmark
    public void insertCompressedSmallOpNoTx() {
        mapNoTransactionsComp.set(KEY, SHORTDATA);
    }

    @Benchmark
    public void insertCompressed1KBOpNoTx() {
        mapNoTransactionsComp.set(KEY, data1KB);
    }


    
    @Benchmark
    public void getSmallOp(Blackhole bh) {
        bh.consume(mapNoTransactions.get(KEY_PERM_SMALL));
    }
    
    @Benchmark
    public void get1KBOp(Blackhole bh) {
        bh.consume(mapNoTransactions.get(KEY_PERM_1KB));
    }
    
    @Benchmark
    public void getSmallCompressedOp(Blackhole bh) {
        bh.consume(mapNoTransactionsComp.get(KEY_PERM_SMALL));
    }
    
    @Benchmark
    public void get1KBCompressedOp(Blackhole bh) {
        bh.consume(mapNoTransactionsComp.get(KEY_PERM_1KB));
    }


    
    @Benchmark
    public void insertGetOpNoTx(Blackhole bh) {
        mapNoTransactions.set(KEY, SHORTDATA);
        bh.consume(mapNoTransactions.get(KEY));
    }

    @Benchmark
    public void insertDeleteOpNoTx() {
        mapNoTransactions.set(KEY, SHORTDATA);
        mapNoTransactions.delete(KEY);
    }
    
    @Benchmark
    public void insertCommitOp() {
        mapWithTransactions.set(KEY, SHORTDATA);
        transaction.commit();
    }
    
    @Benchmark
    public void insertCommitGetOp(Blackhole bh) {
        mapWithTransactions.set(KEY, SHORTDATA);
        transaction.commit();
        bh.consume(mapWithTransactions.get(KEY));
    }

    @Benchmark
    public void insertDeleteCommitOp() {
        mapWithTransactions.set(KEY, SHORTDATA);
        mapWithTransactions.delete(KEY);
        transaction.commit();
    }
    
    @Benchmark
    public void deleteOpNoTx() {
        mapNoTransactions.delete(KEY);
    }
    
    @Benchmark
    public void deleteOpWithTx() {
        mapWithTransactions.delete(KEY);
        transaction.commit();
    }

}
