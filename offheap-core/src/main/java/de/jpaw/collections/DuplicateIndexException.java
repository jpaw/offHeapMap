package de.jpaw.collections;

/** Thrown if an operation would create a second index for a different key where the index
 * has been defined as unique.
 *
 */
public class DuplicateIndexException extends RuntimeException {
    private static final long serialVersionUID = 2022906979901590681L;

    public DuplicateIndexException(String table, long key, Object index) {
        super("Index " + index.toString() + " (for key " + key + ") already exists in " + table);
    }

    public DuplicateIndexException(Throwable cause, String msg) {
        super(msg, cause);
    }

    // temporary throw from JNI
    public DuplicateIndexException(String msg) {
        super(msg);
    }
}
