package de.jpaw.offHeap;

/** A shard is a fixed group of one or many maps. The shard's purpose is to allow a fast assignment of
 * maps to a transaction. A shard has no corresponding object in the JBI layer. 
 * 
 * @author Michael Bischoff
 *
 */
public class Shard {
    
    // private final List<LongToByteArrayOffHeapMap> participatingMaps = new ArrayList<>(20);
    
    private volatile OffHeapTransaction owningTransaction = null;

    public final static Shard TRANSACTIONLESS_DEFAULT_SHARD = new Shard();

    public long getTxCStruct() {
        OffHeapTransaction tx = owningTransaction;
        return tx == null ? 0L : tx.getCStruct();
    }
    
    public OffHeapTransaction getOwningTransaction() {
        return owningTransaction;
    }

    public void setOwningTransaction(OffHeapTransaction owningTransaction) {
        if (this == TRANSACTIONLESS_DEFAULT_SHARD)
            throw new RuntimeException("Cannot change transaction of default shard");
        this.owningTransaction = owningTransaction;
    }
}
