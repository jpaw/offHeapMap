package de.jpaw.offHeap;

import java.util.concurrent.atomic.AtomicBoolean;

public class OffHeapTransaction {
    // bitmasks for the transaction parameter - if all are 0, no transactions will be done
    public static int TRANSACTIONAL = 0x01;     // allow to rollback / safepoints
    public static int REDOLOG_ASYNC = 0x02;     // allow replaying on a different database - fast
    public static int REDOLOG_SYNC = 0x04;      // allow replaying on a different database - safe
    
    //
    // internal native API
    //
    
    
    /** Start a new transaction, using a given reference, with optional redo log writing. */
    private native void natStartTransaction(long ref, int modes);
    
    /** Commit a pending transaction. (Returns the number of low level DB row operations) */
    private native int natCommit();
    
    /** Rollback a pending transaction (possibly only to a safepoint). */
    private native void natRollback(int toWhere);
    
    /** Define a safepoint. */
    private native int natSetSafepoint();
    
    
    public static final OffHeapTransaction INSTANCE = new OffHeapTransaction();

    
    private AtomicBoolean active = new AtomicBoolean();
    private int currentMode = 0;
    private int lastSafepoint = 0;
    
    private OffHeapTransaction() {
    }

    public void startTransaction(long ref, int modes) {
        if (active.getAndSet(true))
            throw new RuntimeException("Transaction already active");
        currentMode = modes;
        lastSafepoint = 0;
        natStartTransaction(ref, modes);
    }
    
    public void rollback() {
        if ((currentMode & TRANSACTIONAL) == 0)
            throw new RuntimeException("Not running in transactional mode");
        if (!active.getAndSet(false))
            throw new RuntimeException("No transaction active");
        natRollback(0);
    }

    public void setSafepoint() {
        lastSafepoint = natSetSafepoint();
    }
    
    public void rollbackToSafepoint() {
        natRollback(lastSafepoint);
    }

}
