package de.jpaw.collections;

public class InconsistentIndexException extends RuntimeException {
    private static final long serialVersionUID = 202290697935190681L;

    public InconsistentIndexException(String table, long key, Object index, String msg) {
        super("Index " + index.toString() + " (for key " + key + ") inconsistency in " + table + ": " + msg);
    }

    public InconsistentIndexException(Throwable cause, String msg) {
        super(msg, cause);
    }

    // temporary throw from JNI
    public InconsistentIndexException(String msg) {
        super(msg);
    }
}
