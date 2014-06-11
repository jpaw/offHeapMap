package de.jpaw.offHeap;

/** Implements a long -> byte [] hash map off heap, using JNI. */
public class LongToByteArrayOffHeapMap {
    private static Boolean isInitialized = Boolean.FALSE;
    
    private long cStruct = 0L;  // only used by native code
    
    public LongToByteArrayOffHeapMap(int size, int maxUncompressed) {
        synchronized (isInitialized) {
            if (!isInitialized)
                System.loadLibrary("lz4"); // Load native library at runtime
                System.loadLibrary("jpawMap"); // Load native library at runtime
            // now we are (unless an Exception was thrown)
            isInitialized = Boolean.TRUE;
        }
        natMake(size, maxUncompressed); 
    }
    
    // native functions
    private native void natMake(int size, int maxUncompressed);  // called from constructor
    public native void close();
    public native byte [] get(long key);
    public native void set(long key, byte [] data);
    public native byte [] put(long key, byte [] data);  // set and return previous
    
}
