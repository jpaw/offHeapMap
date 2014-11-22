// forwards...

// in map
void throwOutOfMemory(JNIEnv *env);
void throwAny(JNIEnv *env, char *msg);

void commitToView(struct tx_log_entry *ep, jlong transactionReference);
void rollback(struct tx_log_entry *ep);
void print(struct tx_log_entry *ep, int i);

// in transactions
struct tx_log_entry *getTxLogEntry(JNIEnv *env, struct tx_log_hdr *ctx);
