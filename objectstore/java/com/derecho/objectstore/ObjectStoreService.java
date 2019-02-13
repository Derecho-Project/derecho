package com.derecho.objectstore;

/**
 * @author xinzheyang
 * ObjectStoreService is a wrapper class for the C++ native ObjectStore
 */
public class ObjectStoreService {
	static {
		System.loadLibrary("oss-jni");
		
	}
	
	private static ObjectStoreService oss;
	private String argv;
	
	/**
	 * @param argv the command line argument to be passed into native implementation
	 */
	private ObjectStoreService(String argv) {
		this.argv = argv;
	}

	/**
	 * @param argv the command line argument to be passed into native implementation
	 * @return a singleton instance of ObjectStoreService
	 */
	public static ObjectStoreService getObjectStoreService(String argv) {
		if (oss == null) {
			oss = new ObjectStoreService(argv);
			oss.initialize(argv);
		}
		return oss;
	}
	
	
	/**
	 * initializes an OSS using the native implementation 
	 */
	private native void initialize(String argv);
	
	/**
	 * @param oid the object id in the storage
	 * @param data the data to be put 
	 * the put operation
	 */
	public native void put(long oid, String data);
	
	
	/**
	 * @param oid the object id to be removed 
	 * @return true if the operation succeeds, false otherwise
	 * the remove operation
	 */
	public native boolean remove(long oid);
	
	
	/**
	 * @param oid the object id to get 
	 * @return the data whose object id is oid
	 * the get operation
	 */
	public native String get(long oid);
	
	
	/**
	 * the leave operation
	 */
	public native void leave();
	
	
}
