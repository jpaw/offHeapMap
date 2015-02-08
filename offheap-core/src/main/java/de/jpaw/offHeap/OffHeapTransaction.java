package de.jpaw.offHeap;

/** An OffHeapTransaction is assigned a collection of shards which participate.
 * A transaction is always "open", there is no need to start one.
 * Assignment of shards however can only be done while no uncommitted data exists.
 * Committing or rolling back a transaction will immediately start the next one. This avoid an additional JNI call.
 * 
 * @author Michael Bischoff
 *
 */
public class OffHeapTransaction {
    // bitmasks for the transaction parameter - if all are 0, no transactions will be done
    public static int TRANSACTIONAL = 0x01;     // allow to rollback / safepoints
    public static int REDOLOG_ASYNC = 0x02;     // allow replaying on a different database - fast
    public static int REDOLOG_SYNC = 0x04;      // allow replaying on a different database - safe
    
    //
    // internal native API
    //
    
    static {
        OffHeapInit.init();
    }
    
    /** Set up the JNI data structures for a new transaction. */
    private native long natCreateTransaction(int initialMode, long changeNumberDelta);
    
    /** Set the logging modes of a transaction. Throws an exception if pending data is in the buffer. */
    private native void natSetMode(long cTx, int modes);
    
    /** Start a transaction / provide a new system change number. Calling this method is not required if the new change number should be the old plus
     * a fixed delta. Throws an exception if updating operations have been performed already. */
    private native void natBeginTransaction(long cTx, long ref);
    
    /** Commit a pending transaction and replay the changes to the committedView (if there is any).
     * (Returns the number of low level DB row operations) */
    private native int natCommit(long cTx);
    
    /** Commit a pending transaction and store the changes for later replay on the committedView (if there is any).
     * Returns a pointer to the store. */
    private native long natCommitDelayedUpdate(long cTx);
    
    /** Replay previously committed changes to the view. Returns the number of rows updated. */
    private native int natUpdateViews(long cTx, long txPtr);
    
    /** Rollback a pending transaction (possibly only to a safepoint). */
    private native void natRollback(long cTx, int toWhere);
    
    /** Define a safepoint. */
    private native int natSetSafepoint(long cTx);
    
    /** Removes a transaction from memory. */
    private native void natCloseTransaction(long cTx);

    /** Prints a log of the redo / rollback table. For debugging. */
    private native void natDebugRedoLog(long cTx);
    
    /** Only used by native code, to store the off heap address of the structure. */
    private long cStruct;
    private int currentMode = 0;
    private int lastSafepoint = 0;
    private int rowsChanged = 0;
    
    protected long getCStruct() {
        return cStruct;  // for the Shard
    }
    
    public OffHeapTransaction(int initialMode) {
        currentMode = initialMode;
        cStruct = natCreateTransaction(initialMode, 1L);
    }
    
    /** Starts a new transaction. */
    public void beginTransaction(long ref) {
        natBeginTransaction(cStruct, ref);
    }

    /** Undoes everything of the current transaction, irrespectively of any safepoint. */
    public void rollback() {
        if ((currentMode & TRANSACTIONAL) == 0)
            throw new RuntimeException("Not running in transactional mode");
        natRollback(cStruct, 0);
        lastSafepoint = 0;
    }

    /** Commits everything of the current transaction. */
    public void commit() {
        rowsChanged += natCommit(cStruct);
    }
    
    /** Sets a safepoint at the current position (simple API). Allows to rollback to this position. */
    public void setSafepoint() {
        lastSafepoint = natSetSafepoint(cStruct);
    }
    
    /** Rolls back any change up to the previous set safepoint (simple API). With this API, no nested safepoints are possible. */ 
    public void rollbackToSafepoint() {
        natRollback(cStruct, lastSafepoint);
    }
    
    /** Sets an additional safepoint. Returns the new safepoint location. With this API, nested safepoints are possible,
     * managed by the application. This operation does not affect the simple API's safepoint position. */
    public int defineSafepoint() {
        return natSetSafepoint(cStruct);
    }

    /** Rolls back any change up to an application managed safepoint (obtained via some earlier call to defineSafepoint). */ 
    public void rollbackToDefinedSafepoint(int position) {
        if (lastSafepoint > position) {
            // invalidate previous simple API call safepoint
            lastSafepoint = position;
        }
        natRollback(cStruct, position);
    }
    
    /** Closes the transaction object. After this call, no more operations are possible. Perform this at application shutdown time. */
    public void close() {
        natCloseTransaction(cStruct);
        cStruct = 0L;
    }
    
    public void printRedoLog() {
        natDebugRedoLog(cStruct);        
    }
    
    /** Commit a pending transaction and store the changes for later replay on the committedView (if there is any).
     * Returns a pointer to the store, or 0L if there is nothing to store. */
    public long commitDelayedUpdate() {
        return natCommitDelayedUpdate(cStruct);
    }
    
    /** Replay previously committed changes to the view.
     * Throws an Exception if the transactions are not replyed in order. */
    public int updateViews(long transactions) {
        return transactions != 0L ? natUpdateViews(cStruct, transactions) : 0;
    }
  
}
