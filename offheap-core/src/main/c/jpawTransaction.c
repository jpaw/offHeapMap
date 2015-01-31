#include "jpawTransaction.h"
#include "globalDefs.h"
#include "globalMethods.h"




struct tx_delayed_update {
    jlong lastCommittedRef;         // this must be the predecessor
    jlong currentTransactionRef;
    int numberOfChanges;            // how many rows have been affected?
    int padding;
    struct tx_log_entry transactions [];
};


// transactions

struct tx_log_entry *getTxLogEntry(JNIEnv *env, struct tx_log_hdr *ctx) {
    int chunkIndex = ctx->number_of_changes >> 8;
    if (chunkIndex >= TX_LOG_ENTRIES_PER_CHUNK_LV1) {
        throwAny(env, "Too many row changes within transaction");
        return NULL;
    }
    struct tx_log_list *chunk = ctx->chunks[chunkIndex];
    if (!chunk) {
        // must allocate
        chunk = malloc(sizeof(struct tx_log_list));
        if (!chunk) {
            throwOutOfMemory(env);
            return NULL;
        }
        ctx->chunks[chunkIndex] = chunk;
    }
    return &(chunk->entries[ctx->number_of_changes & 0xff]);
}

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCreateTransaction
 * Signature: (IJ)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCreateTransaction
  (JNIEnv *env, jobject tx, jint mode, jlong changeNumberDelta) {
    struct tx_log_hdr *hdr = malloc(sizeof(struct tx_log_hdr));
    if (!hdr) {
        throwOutOfMemory(env);
        return (jlong)0;
    }
    memset(hdr, 0, sizeof(struct tx_log_hdr));
    hdr->modes = mode;
    hdr->number_of_changes = 0;
    hdr->lastCommittedRef = (jlong)-1L;
    hdr->lastCommittedRefOnViews = (jlong)-1L;
    hdr->changeNumberDelta = changeNumberDelta;
    hdr->currentTransactionRef = changeNumberDelta;
#ifdef DEBUG
    fprintf(stderr, "CREATE TRANSACTION\n");
#endif
    return (jlong)hdr;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCloseTransaction
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCloseTransaction
  (JNIEnv *env, jobject me, jlong cTx) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    if (hdr->number_of_changes) {
        throwAny(env, "Cannot close within pending transaction");
        return;
    }
    // free redo blocks
    int i;
    for (i = 0; i < TX_LOG_ENTRIES_PER_CHUNK_LV1 && hdr->chunks[i]; ++i) {
        free(hdr->chunks[i]);
    }
    free(hdr);
#ifdef DEBUG
    fprintf(stderr, "CLOSE TRANSACTION\n");
#endif
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natSetMode
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natSetMode
  (JNIEnv *env, jobject me, jlong cTx, jint mode) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    if (hdr->number_of_changes) {
        throwAny(env, "Cannot change mode within pending transaction");
        return;
    }
    hdr->modes = mode;
}

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natBeginTransaction
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natBeginTransaction
(JNIEnv *env, jobject me, jlong cTx, jlong newRef) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    if (hdr->number_of_changes) {
        throwAny(env, "Cannot begin transaction while changes are pending");
        return;
    }
    hdr->currentTransactionRef = newRef;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natSetSafepoint
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natSetSafepoint
  (JNIEnv *env, jobject me, jlong cTx) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    return hdr->number_of_changes;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCommitDelayedUpdate
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCommitDelayedUpdate
    (JNIEnv *env, jobject me, jlong cTx) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    int number_of_changes = hdr->number_of_changes;
    if (!number_of_changes) {
        return (jlong)0;
    }
    struct tx_delayed_update *upd = malloc(sizeof(struct tx_delayed_update) + sizeof(struct tx_log_entry) * number_of_changes);
    if (!upd) {
        throwOutOfMemory(env);
        return (jlong)0;
    }
    upd->lastCommittedRef = hdr->lastCommittedRef;
    upd->currentTransactionRef = hdr->currentTransactionRef;
    upd->numberOfChanges = number_of_changes;
    upd->padding = 0;

    int i = 0;
    int j = 0;  // slot number
    while (i + TX_LOG_ENTRIES_PER_CHUNK_LV2 < number_of_changes) {  // a loop for all but the last one
        memcpy(&(upd->transactions[i]), hdr->chunks[j], sizeof(struct tx_log_entry) * TX_LOG_ENTRIES_PER_CHUNK_LV2);
        ++j;
        i += TX_LOG_ENTRIES_PER_CHUNK_LV2;
    }
    memcpy(&(upd->transactions[i]), hdr->chunks[j], sizeof(struct tx_log_entry) * (number_of_changes - i));
    hdr->number_of_changes = 0;
    hdr->lastCommittedRef = hdr->currentTransactionRef;
    hdr->currentTransactionRef += hdr->changeNumberDelta;
    return (jlong)upd;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natUpdateViews
 * Signature: (JJ)V
 */
JNIEXPORT int JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natUpdateViews
(JNIEnv *env, jobject me, jlong cTx, jlong transactions) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    struct tx_delayed_update *upd = (struct tx_delayed_update *)transactions;
    if (upd->lastCommittedRef != hdr->lastCommittedRefOnViews) {
        fprintf(stderr, "current = %ld, lastCommitted = %ld, last committed on views = %ld\n",
                (long)hdr->currentTransactionRef, (long)hdr->lastCommittedRef, (long)hdr->lastCommittedRefOnViews);
        throwAny(env, "Invalid sequence of replays");
        return 0;
    }
    int numberOfChanges = upd->numberOfChanges;
    int i;
    struct tx_log_entry *ep = upd->transactions;
    for (i = 0; i < numberOfChanges; ++i) {
        commitToView(ep++, hdr->currentTransactionRef);
    }

    hdr->lastCommittedRefOnViews = upd->currentTransactionRef;
    free(upd);
    return numberOfChanges;
}


/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natCommit
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natCommit
  (JNIEnv *env, jobject me, jlong cTx) {
    // currently no redo logs are written. Therefore just discard the entries
#ifdef DEBUG
    fprintf(stderr, "COMMIT START\n");
#endif
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;


    int currentEntries = hdr->number_of_changes;
    if (currentEntries) {
        if (hdr->lastCommittedRef != hdr->lastCommittedRefOnViews) {
            fprintf(stderr, "current = %ld, lastCommitted = %ld, last committed on views = %ld\n",
                    (long)hdr->currentTransactionRef, (long)hdr->lastCommittedRef, (long)hdr->lastCommittedRefOnViews);
            throwAny(env, "Invalid sequence of commit");
            return 0;
        }

        struct tx_log_list *chunk = NULL;
        int i;
        for (i = 0; i < currentEntries; ++i) {
            if (!(i & 0xff)) {
                // need a new chunk
                chunk = hdr->chunks[i >> 8];
            }
            commitToView(&(chunk->entries[i & 0xff]), hdr->currentTransactionRef);
        }
    }
    hdr->number_of_changes = 0;
    hdr->lastCommittedRef = hdr->currentTransactionRef;
    hdr->lastCommittedRefOnViews = hdr->currentTransactionRef;
    hdr->currentTransactionRef += hdr->changeNumberDelta;
#ifdef DEBUG
    fprintf(stderr, "COMMIT END\n");
#endif
    return currentEntries;
}



/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natDebugRedoLog
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natDebugRedoLog
  (JNIEnv *env, jobject me, jlong cTx) {
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    int currentEntries = hdr->number_of_changes;
    int i;
    for (i = 0; i < currentEntries; ++i) {
        struct tx_log_list *chunk = hdr->chunks[i >> 8];
        print(&chunk->entries[i & 0xff], i);
    }
}

/*
 * Class:     de_jpaw_offHeap_OffHeapTransaction
 * Method:    natRollback
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_de_jpaw_offHeap_OffHeapTransaction_natRollback
  (JNIEnv *env, jobject me, jlong cTx, jint rollbackTo) {
#ifdef DEBUG
    fprintf(stderr, "ROLLBACK START (%d)\n", rollbackTo);
#endif
    struct tx_log_hdr *hdr = (struct tx_log_hdr *) cTx;
    int currentEntries = hdr->number_of_changes;
    if (currentEntries > rollbackTo) {
        struct tx_log_list *chunk = hdr->chunks[currentEntries >> 8];
        while (currentEntries > rollbackTo) {
            if (!(currentEntries & 0xff))
                chunk = hdr->chunks[(currentEntries-1) >> 8];
            --currentEntries;
            rollback(&(chunk->entries[currentEntries & 0xff]));
        }
        hdr->number_of_changes = rollbackTo;
    }
#ifdef DEBUG
    fprintf(stderr, "ROLLBACK END\n");
#endif
}

