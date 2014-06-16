package de.jpaw.offHeap;

import java.util.concurrent.atomic.AtomicLong;

/** An OffHeapTransaction is assigned a collection of shards which participate.
 * A transaction is always "open", there is no need to start one.
 * Assignment of shards however can only be done while no uncommitted data exists.
 * Committing or rolling back a transaction will immediately start the next one. This avoid an additional JNI call.
 * 
 * @author Michael Bischoff
 *
 */
public class OffHeapTransaction {
    private static final AtomicLong transactionRef = new AtomicLong(0L);    // the previous system change number
    
    // bitmasks for the transaction parameter - if all are 0, no transactions will be done
    public static int TRANSACTIONAL = 0x01;     // allow to rollback / safepoints
    public static int REDOLOG_ASYNC = 0x02;     // allow replaying on a different database - fast
    public static int REDOLOG_SYNC = 0x04;      // allow replaying on a different database - safe
    
    //
    // internal native API
    //
    
    static {
        OffHeapInit.init();
        natInit();
    }
    
    /** Register globals. */
    private native static void natInit();
    
    /** Set up the JNI data structures for a new transaction. */
    private native long natCreateTransaction(int initialMode);
    
    /** Set the logging modes of a transaction. Throws an exception if pending data is in the buffer. */
    private native void natSetMode(int modes);
    
    /** Commit a pending transaction. (Returns the number of low level DB row operations) */
    private native int natCommit(long ref);
    
    /** Rollback a pending transaction (possibly only to a safepoint). */
    private native void natRollback(int toWhere);
    
    /** Define a safepoint. */
    private native int natSetSafepoint();
    
    /** Removes a transaction from memory. */
    private native void natCloseTransaction();

    /** Prints a log of the redo / rollback table. For debugging. */
    private native void natDebugRedoLog();
    
    /** Only used by native code, to store the off heap address of the structure. */
    protected final long cStruct;
    private int currentMode = 0;
    private int lastSafepoint = 0;
    private int rowsChanged = 0;
    
    
    public OffHeapTransaction(int initialMode) {
        currentMode = initialMode;
        cStruct = natCreateTransaction(initialMode);
    }

    public void rollback() {
        if ((currentMode & TRANSACTIONAL) == 0)
            throw new RuntimeException("Not running in transactional mode");
        natRollback(0);
        lastSafepoint = 0;
    }

    public void setSafepoint() {
        lastSafepoint = natSetSafepoint();
    }
    
    public void rollbackToSafepoint() {
        natRollback(lastSafepoint);
    }

    public void commit() {
        rowsChanged += natCommit(transactionRef.incrementAndGet());
    }
    public void commit(long ref) {
        rowsChanged += natCommit(ref);
    }
    public void close() {
        natCloseTransaction();
    }
    
    public void printRedoLog() {
        natDebugRedoLog();        
    }
}
