package de.jpaw.offHeap;

/** Whole purpose of this class is to provide a central address for loading the JNI libraries. */

public class OffHeapInit {
    static {
        System.loadLibrary("lz4");      // Load native library at runtime
        System.loadLibrary("jpawMap");  // Load native library at runtime
    }

    // just a hook to perform the loading
    public static void init() {
    }

    // constructor is private to avoid that any instances are created.
    private OffHeapInit() {
    }
}
