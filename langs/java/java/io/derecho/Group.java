package io.derecho;

import java.nio.ByteBuffer;
import java.util.List;

/** The Derecho Java API implementation. */
public class Group {

  /**
   * All Derecho Java applications should implement a class of this interface. This interface
   * specializes the functionalities users intend to do after the message is delivered, or when
   * message is persisted, locally or globally. see c++ code @
   * derecho/include/derecho/core/detail/multicast_group.hpp
   */
  public interface ICallbackSet {

    /**
     * The callback to be implemented by the user for what to do after the message is stabilized
     * (delivered in order).
     *
     * @param subgroup_id The subgroup ID to receive the message.
     * @param node_id The node ID to receive the message.
     * @param message_id The message ID of the message received.
     * @param version The version number of the view.
     * @param data The content of the message.
     */
    void global_stability_callback(
        int subgroup_id, int node_id, long message_id, long version, ByteBuffer data);

    /**
     * The callback to be implemented by the user for what to do after the message is put into the
     * local persistent storage.
     *
     * @param subgroup_id The subgroup ID to receive the message.
     * @param version The version number of the view.
     */
    void local_persistence_callback(int subgroup_id, long version);

    /**
     * The callback to be implemented by the user for what to do after the message is put into the
     * global persistent storage.
     *
     * @param subgroup_id The subgroup ID to receive the message.
     * @param version The version number of the view.
     */
    void global_persistence_callback(int subgroup_id, long version);
  };

  /** This will be updated by the derecho view upcall */
  private View view;
  /** The group handle, used by the jni code to find the C/C++ group instance. */
  private long handle;
  /** Subgroup object registry, used by the jni code to find subgroup object. */
  private static SubgroupObjectRegistry subgroupObjectRegistry =
      SubgroupObjectRegistry.getInstance();
  /** List of classes. Used to find class - subgroup ID correspondence. */
  private List<Class<?>> listOfClasses;
  /** Callback Set to call after certain states of the group is reached. */
  private ICallbackSet callback_set;

  /**
   * The Constructor of the Java Group.
   *
   * @param subgroupTypes list of classes to define subgroups
   * @param shardViewGenerator user-defined view generator that generates the shard view of the
   *     group
   * @param callbackSet user-defined set of callbacks to be called after certain states of the group
   *     is reached
   */
  public Group(
      List<Class<?>> subgroupTypes,
      IShardViewGenerator shardViewGenerator,
      ICallbackSet callbackSet) {
    System.out.println("About to Create group ");
    this.handle = createGroup(subgroupTypes, shardViewGenerator, callbackSet);
    this.listOfClasses = subgroupTypes;
  }

  /**
   * Creates the derecho group.
   *
   * @param subgroupTypes user-defaulted subgroup Types.
   * @param shardViewGenerator user-defined shard view generator that generates the shard view of
   *     the group.
   * @param callbackSet user-defined set of callbacks to be called after certain states of the group
   *     is reached
   * @return the handle to the group. Failed to create a group when value is less than 0.
   */
  public native long createGroup(
      List<Class<?>> subgroupTypes,
      IShardViewGenerator shardViewGenerator,
      ICallbackSet callbackSet);

  /** barrier sync */
  public native void barrierSync();

  /**
   * leaving a group
   *
   * @param shutdown True if all nodes are leaving.
   */
  public native void leave(boolean shutdown);

  /** leaving a group */
  public void leave() {
    leave(true);
  }

  /**
   * Get the rank of my node.
   *
   * @return rank
   */
  public native Integer get_my_rank();

  /**
   * Get the members of my current group.
   *
   * @return the member list
   */
  public native List<Integer> getMembers();

  /**
   * Get the handle to a subgroup, by type and subgroup Id
   *
   * @param cls the class to get subgroup. Requires that the class should exist in the group shard
   *     view.
   * @param subgroupIndex the subgroup ID of the subgroup. Subgroup ID follows the sequence of
   *     adding subgroups into the shard view. Requires that the subgroup index should be valid in
   *     the group.
   * @return a Replicated object that can be used to send(), ordered_send(), or p2p_send().
   */
  public Replicated getSubgroup(Class<?> cls, int subgroupIndex) {

    Object obj = subgroupObjectRegistry.getObj(subgroupIndex);

    assert obj.getClass().equals(cls) : "Replicated.getSubgroup : class does not match object.";

    assert obj.getClass().equals(listOfClasses.get(subgroupIndex))
        : "Replicated.getSubgroup : class does not match list.";

    return new Replicated(subgroupIndex, handle, this);
  }

  /**
   * Get the handle to a subgroup, by type only.
   *
   * @param cls the class to get subgroup. If there are multiple subgroups with this class in the
   *     group, we will get the first subgroup satisfying the condition.
   * @return a Replicated object that can be used to send(), ordered_send(), or p2p_send(), or
   *     [null] if no such subgroup with the given class [cls] exists.
   */
  public Replicated getSubgroup(Class<?> cls) {

    if (!listOfClasses.contains(cls)) return null;
    return new Replicated(listOfClasses.indexOf(cls), handle, this);
  }
}
