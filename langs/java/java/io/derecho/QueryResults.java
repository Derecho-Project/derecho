package io.derecho;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * The handle that stores the future of Java ordered_send() and p2p_send() natives. Java side should
 * call get() in this class to get the objects returned.
 */
public class QueryResults {

  /**
   * The <node ID, object> map that stores <object>, the return objects from ordered_send() or
   * p2p_send() in each node with <node ID>.
   */
  Map<Integer, Object> replyMap;
  /** The indicator of whether the objects are updated. */
  boolean updated;
  /** The handle that stores the C++ memory address of the query results future. */
  long handle;

  // 0 - Ordered send
  // 1 - p2p send
  int mode;

  /** The subgroup ID of the subgroup to deliver messages while using p2p_send(). */
  int nid;

  /**
   * Ordered_send() should use this constructor.
   *
   * @param h the handle to store the C++ memory address of the query results future.
   * @param m The mode of delivery. 0 for ordered_send.
   */
  QueryResults(long h, int m) {

    replyMap = null;
    updated = false;
    handle = h;
    mode = m;
  }

  /**
   * p2p_send() should use this constructor.
   *
   * @param h the handle to store the C++ memory address of the query results future.
   * @param m The mode of delivery. 1 for p2p_send.
   * @param nid The node ID to deliver message in p2p_send().
   */
  QueryResults(long h, int m, int nid) {

    replyMap = null;
    updated = false;
    handle = h;
    mode = m;
    this.nid = nid;
  }

  /**
   * Get the results of the query when the future that stores the results of calling Java functions
   * is available.
   *
   * @return The [node ID, object] map that stores [object], the return objects from ordered_send()
   *     or p2p_send() in each node with [node ID]. If p2p_send() does not modify a node, then the
   *     value corresponding to that node is not changed (maybe null if it is never initialized).
   *     The map will not contain node ID [node ID] if the node has not replied with the object yet.
   */
  public Map<Integer, Object> get() {

    if (mode == 0) {
      this.getOS();
    } else if (mode == 1) {
      this.getp2p();
    } else {
      throw new Error();
    }

    assert (replyMap != null);
    updated = true;

    return replyMap;
  }

  /** Get the results for ordered_send(). */
  private void getOS() {

    replyMap = new HashMap<>();
    Map<Integer, byte[]> rmap = this.getReplyMapOS(handle);

    for (int i : rmap.keySet()) {

      byte[] arr = rmap.get(i);

      Object o = Replicated.deserialize(arr);
      replyMap.put(i, o);
    }
  }

  /** Get the results for p2p_send(). */
  private void getp2p() {

    replyMap = new HashMap<>();
    byte[] res = this.getReplyMapP2P(handle, nid);

    if (res != null) replyMap.put(nid, Replicated.deserialize(res));
  }

  /**
   * Get the reply map of ordered_send(). A native function.
   *
   * @param handle The memory address that stores the C++ query_results object.
   * @return a map that stores [node_id, byte objects] pair that would be used to get the Java query
   *     results object.
   */
  private native Map<Integer, byte[]> getReplyMapOS(long handle);

  /**
   * Get the reply map of p2p_send(). A native function.
   *
   * @param handle The memory address that stores the C++ query_results object.
   * @param targetNodeID the target node ID of sending the p2p message.
   * @return a map that stores [node_id, byte objects] pair that would be used to get the Java query
   *     results object.
   */
  private native byte[] getReplyMapP2P(long handle, int targetNodeID);
}
