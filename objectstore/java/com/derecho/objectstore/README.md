# Java Wrapper for Derecho ObjectStore API
The wrapper is written using Java Native Interface (JNI) to support API calls from Java programs.

## Build
The Java API is incorporated in the Derecho building system. So, it is built together with the whole derecho project, including the `jar`s.
After `cmake`, in the directory `build/objectstore/java/com/derecho/objectstore/`, a C++ library `liboss-jni.so` can be found, which is the library that interacts with the JNI layer.
In the same directory, a Java jar called `oss.jar`. This is the Java API for ObjectStoreService and ready for Java programmers to use.

## Configure and Run
* Set the environment variable `JAVA_HOME` to the JDK installation library. See how to [here](https://docs.oracle.com/cd/E19182-01/821-0917/6nluh6gq9/index.html).
* Just as any other Derecho applications, a derecho config file is required, including the special configurations for ObjectStore (default config can be found at `objectstore/objectstore-default.cfg`).
* The Java system library path needs to be specified as the directory containing `liboss-jni.so` mentioned above. This can be done by setting the environment variable `LD_LIBRARY_PATH`.
For example,
```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/liboss-jni.so
```

`OSSTest.java` is a simple demo illustrating how to call ObjectStoreService from Java.
```
java -jar com.derecho.objectstore.OSSTest
```
