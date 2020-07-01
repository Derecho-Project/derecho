package io.derecho;

import java.io.*;
import java.lang.reflect.Method;

/** The handle to call ordered_send(), p2p_send(), and raw send(). */
public class Replicated {

  /** The subgroup ID of the current handle. */
  int sid;
  /** The memory address of the C++ group. */
  long group_handle;
  /** The pointer to the Java group. */
  Group group;

  /**
   * Constructor of Replicated handle.
   *
   * @param subgroup_id The subgroup ID of the current handle.
   * @param group_handle The memory address of the C++ group.
   * @param group The pointer to the Java group.
   */
  Replicated(int subgroup_id, long group_handle, Group group) {

    sid = subgroup_id;
    this.group_handle = group_handle;

    this.group = group;
  }

  /**
   * Deserializes the byte array [str] into a Java object.
   *
   * @param str the byte array to deserialize.
   * @return the object deserialized from [str], or [null] if that is impossible.
   */
  public static Object deserialize(byte[] str) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(str);
      ObjectInputStream ois = new ObjectInputStream(bais);
      return ois.readObject();
    } catch (IOException e) {

      e.printStackTrace();

      return null;

    } catch (ClassNotFoundException e) {
      e.printStackTrace();

      return null;
    }
  }

  /**
   * Serializes the object [o] into a byte array using Java's default serializing mechanism.
   *
   * @param o the object to serialize
   * @return a byte array that stores the results of serialization, or [null] if that is not
   *     possible.
   */
  public static byte[] serialize(Serializable o) {
    try {
      if (o == null) return null;

      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      ObjectOutputStream oos = new ObjectOutputStream(baos);

      oos.writeObject(o);

      return baos.toByteArray();
    } catch (IOException e) {

      e.printStackTrace();

      return null;

    } catch (Exception e) {

      e.printStackTrace();
      return null;
    }
  }

  /**
   * Helper function to call Java method. For internal use.
   *
   * @param o The object to call method.
   * @param name The name of function to call.
   * @param clsObj The list of classes of parameters.
   * @param args The arguments of the function call.
   * @return
   */
  public static Object callJavaMethod(Object o, String name, Class<?>[] clsObj, Object[] args) {
    try {
      Method m = o.getClass().getMethod(name, clsObj);
      m.setAccessible(true);
      Object ret = m.invoke(o, args);

      return ret;

    } catch (Exception e) {
      System.out.println("==================");
      System.out.println("Object: " + o);
      System.out.println("Method: " + name);
      System.out.println("Args: " + args.length + " " + args);

      e.printStackTrace();
      return null;
    }
  }

  /**
   * Raw send primitive in Java.
   *
   * @param payloadSize The size of the payload.
   * @param messageGenerator The object that user implements to write message to the buffer.
   */
  public native void send(long payloadSize, IMessageGenerator messageGenerator);

  /** Ordered Send primitives with fixed number of arguements. */
  public QueryResults ordered_send(Method m) {
    Object[] arr = {};
    return ordered_send(m, arr);
  }

  /**
   * Do ordered send on the current object.
   *
   * @param m method to do ordered send
   * @param args arguments of m. Requires: args should be serializable
   * @return a QueryResults handle that could be used to access return values of Java functions.
   */
  public QueryResults ordered_send(Method m, Object[] args) {

    // serialize args

    byte[] byte_args = serialize(args);

    if (byte_args == null) {
      System.out.println("java null!");
      return null;
    }

    Class<?>[] class_arr = m.getParameterTypes();

    byte[] method_args = serialize(class_arr);

    long handler = ordered_send(m.getName(), method_args, byte_args);

    return new QueryResults(handler, 0);
  }

  /** Ordered Send primitives with fixed number of arguements. */
  public QueryResults ordered_send(Method m, Serializable a) {
    Object[] arr = {a};
    return ordered_send(m, arr);
  }

  /** Ordered Send primitives with fixed number of arguements. */
  public QueryResults ordered_send(Method m, Serializable a, Serializable b) {
    Object[] arr = {a, b};
    return ordered_send(m, arr);
  }

  /** Ordered Send primitives with fixed number of arguements. */
  public QueryResults ordered_send(Method m, Serializable a, Serializable b, Serializable c) {
    Object[] arr = {a, b, c};
    return ordered_send(m, arr);
  }

  /** Ordered Send primitives with fixed number of arguements. */
  public QueryResults ordered_send(
      Method m, Serializable a, Serializable b, Serializable c, Serializable d) {
    Object[] arr = {a, b, c, d};
    return ordered_send(m, arr);
  }

  /** The native ordered_send() function. */
  private native long ordered_send(String funcName, byte[] method_class_args, byte[] method_args);

  /**
   * Do p2p send on the current object.
   *
   * @param cls The target class to send message.
   * @param target The node ID of the target node.
   * @param m the method to do p2p send.
   * @param args a QueryResults handle that could be used to access return values of Java functions.
   */
  public QueryResults p2p_send(Class<?> cls, int target, Method m, Object[] args) {

    // serialize args

    byte[] byte_args = serialize(args);

    Replicated new_replicated = group.getSubgroup(cls);

    Class<?>[] class_arr = m.getParameterTypes();

    byte[] method_args = serialize(class_arr);

    long handler = p2p_send(target, new_replicated.sid, m.getName(), method_args, byte_args);

    return new QueryResults(handler, 1, new_replicated.sid);
  }

  /** p2p send with fixed number of arguements. */
  public QueryResults p2p_send(Class<?> cls, int target, Method m) {
    Object[] arr = {};
    return p2p_send(cls, target, m, arr);
  }

  /** p2p send with fixed number of arguements. */
  public QueryResults p2p_send(Class<?> cls, int target, Method m, Serializable a) {
    Object[] arr = {a};
    return p2p_send(cls, target, m, arr);
  }

  /** p2p send with fixed number of arguements. */
  public QueryResults p2p_send(Class<?> cls, int target, Method m, Serializable a, Serializable b) {
    Object[] arr = {a, b};
    return p2p_send(cls, target, m, arr);
  }

  /** p2p send with fixed number of arguements. */
  public QueryResults p2p_send(
      Class<?> cls, int target, Method m, Serializable a, Serializable b, Serializable c) {
    Object[] arr = {a, b, c};
    return p2p_send(cls, target, m, arr);
  }

  /** p2p send with fixed number of arguements. */
  public QueryResults p2p_send(
      Class<?> cls,
      int target,
      Method m,
      Serializable a,
      Serializable b,
      Serializable c,
      Serializable d) {
    Object[] arr = {a, b, c, d};
    return p2p_send(cls, target, m, arr);
  }

  /** The native p2p_send() function. */
  private native long p2p_send(
      int target_node_id,
      int target_subgroup_id,
      String funcName,
      byte[] method_sig_args,
      byte[] args);
}
