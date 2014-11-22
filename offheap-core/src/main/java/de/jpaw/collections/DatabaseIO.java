package de.jpaw.collections;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** Methods to dump an in-memory database to disk, or to restore one from disk. */
public interface DatabaseIO {
    static public final Charset DEFAULT_FILENAME_ENCODING = StandardCharsets.UTF_8;
    
    /** Dump the full database contents to a disk file. */
    default void writeToFile(String pathname) {
        writeToFile(pathname, DEFAULT_FILENAME_ENCODING);
    }
    
    /** Read a database from disk. The database should be empty before. */
    default void readFromFile(String pathname) {
        readFromFile(pathname, DEFAULT_FILENAME_ENCODING);
    }
    
    /** Dump the full database contents to a disk file. */
    void writeToFile(String pathname, Charset filenameEncoding);
    
    /** Read a database from disk. The database should be empty before. */
    void readFromFile(String pathname, Charset filenameEncoding);
}
