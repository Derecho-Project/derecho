package io.derecho.test;

import io.derecho.Group;
import io.derecho.IMessageGenerator;
import io.derecho.IShardViewGenerator;
import io.derecho.Replicated;
import io.derecho.View;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** [BandwidthTest] tests the bandwidth to send raw messages through Derecho Java. */
public class BandwidthTest implements IShardViewGenerator, Group.ICallbackSet, IMessageGenerator {

  // Load derecho JNI library.
  static {
    try {
      System.loadLibrary("derechojni");
    } catch (UnsatisfiedLinkError e) {
      System.out.println(e.getMessage());
    }
  }

  int num_nodes;
  int delivery_mode; // TODO: not supported right now. Could be done later.
  int num_senders_selector;
  int num_messages;
  AtomicBoolean is_done;
  int counter;

  /** java derecho need a default constructor. */
  BandwidthTest() {}

  /**
   * The official constructor of BandwidthTest.
   *
   * @param number_of_nodes the number of nodes to join the BandwidthTest.
   * @param mode Usually 0 while we are testing. Not used. (ordered send)
   * @param selector 0: all senders. 1: half senders. 2: one sender.
   * @param number_of_message Number of messages to send throughout the test.
   */
  BandwidthTest(int number_of_nodes, int mode, int selector, int number_of_message) {
    this.num_nodes = number_of_nodes;
    this.delivery_mode = mode;
    this.num_senders_selector = selector;
    this.num_messages = number_of_message;
    is_done = new AtomicBoolean(false);
    counter = 0;
  }

  /**
   * After work is done, Java side would call this function to notify Java side that the work is
   * finished and end time should be recorded.
   */
  @Override
  public void global_stability_callback(
      int subgroup_id, int node_id, long message_id, long version, ByteBuffer data) {

    counter++;

    int num_sending_nodes = this.num_nodes;
    if (num_senders_selector == 1) {
      num_sending_nodes /= 2;
    } else if (num_senders_selector == 2) {
      num_sending_nodes = 1;
    }
    if (num_sending_nodes * this.num_messages == counter) {
      this.is_done.set(true);
    }
  }

  @Override
  public void local_persistence_callback(int subgroup_id, long version) {}

  @Override
  public void global_persistence_callback(int subgroup_id, long version) {}

  /**
   * Generate the shard view here. In bandwidth test, all nodes are in one shard, one subgroup, and
   * one group. The class of this group is BandwidthTest.
   */
  @Override
  public Map<Class<?>, List<List<Set<Integer>>>> generate(
      List<Class<?>> subgroupTypes, View previousView, View currentView) {
    int num_members = currentView.members.size();

    // we don't have enough nodes here.
    if (num_members < num_nodes) {
      return null;
    }

    // subgroupList
    Set<Integer> shard = new HashSet<Integer>();
    Iterator<Integer> im = currentView.members.iterator();
    while (im.hasNext()) {
      Integer i = im.next();
      shard.add(i);
    }

    List<Set<Integer>> subgroup = new ArrayList<>();
    subgroup.add(shard);

    List<List<Set<Integer>>> retList = new ArrayList<>();
    retList.add(subgroup);

    Map<Class<?>, List<List<Set<Integer>>>> retMap = new HashMap<>();
    retMap.put(BandwidthTest.class, retList);
    return retMap;
  }

  /** Writes a default message to the buffer so that there could be something to be sent. */
  @Override
  public void write(ByteBuffer buffer) {
    String msg = "Some Messages."; // Just write this.
    buffer.put(msg.getBytes());
  }

  /** Execute the bandwidth test. */
  public static final void main(String[] args) {
    if (args.length != 5) {
      System.out.println(
          "Usage: java -jar "
              + BandwidthTest.class.getName()
              + ".jar num_nodes, num_senders_selector (0 - all senders, 1 - half senders, 2 - one sender), num_messages, delivery_mode (0 - ordered mode, 1 - unordered mode), message_size");
      return;
    }

    // Arguments passed from command line.
    int number_of_nodes = Integer.parseInt(args[0]);
    int num_senders_selector = Integer.parseInt(args[1]);
    int num_messages = Integer.parseInt(args[2]);
    int delivery_mode = Integer.parseInt(args[3]);
    int message_size = Integer.parseInt(args[4]);

    // Initialize bandwidth test and group.
    List<Class<?>> subgroupTypes = new ArrayList<Class<?>>();
    subgroupTypes.add(BandwidthTest.class);
    BandwidthTest bandwidthTest =
        new BandwidthTest(number_of_nodes, delivery_mode, num_senders_selector, num_messages);
    Group group = new Group(subgroupTypes, bandwidthTest, bandwidthTest);
    int node_rank = group.get_my_rank();
    System.out.println("Finished joining group with rank = " + node_rank);

    // Send message.
    Replicated raw_subgroup = group.getSubgroup(BandwidthTest.class, 0);

    boolean is_send =
        num_senders_selector == 0
            || (num_senders_selector == 1 && node_rank > (number_of_nodes - 1) / 2)
            || (num_senders_selector == 2 && node_rank == number_of_nodes - 1);
    System.out.println("I am here");
    // Start Timer
    long start_time = System.nanoTime();
    if (is_send) {
      for (int i = 0; i < num_messages; i++) {
        raw_subgroup.send(message_size, bandwidthTest);
      }
    } else {
      // do nothing
    }

    // Wait until everybody in the group is done.
    try {
      while (!bandwidthTest.is_done.get()) {}

    } catch (Exception e) {
      e.printStackTrace();
      // do nothing
    }

    // End Timer
    long end_time = System.nanoTime();
    long nanoseconds_elapsed = (end_time - start_time);

    final long max_msg_size = message_size;

    // calculate bandwidth measured locally

    double bw;
    if (num_senders_selector == 0) {
      bw = (max_msg_size * num_messages * number_of_nodes + 0.0) / nanoseconds_elapsed;
    } else if (num_senders_selector == 1) {
      System.out.println(number_of_nodes / 2);
      bw = (max_msg_size * num_messages * (number_of_nodes / 2) + 0.0) / nanoseconds_elapsed;
    } else {
      bw = (max_msg_size * num_messages + 0.0) / nanoseconds_elapsed;
    }

    if (node_rank == 0) {
      System.out.println(
          "Num of nodes: "
              + number_of_nodes
              + "\nNumber of senders: "
              + num_senders_selector
              + "\nMax message size: "
              + max_msg_size
              + "\nNumber of messages: "
              + num_messages
              + "\nBandwidth: "
              + bw);
    }

    // Leave the group.
    System.out.println("Leaving group...");
    group.barrierSync();
    group.leave();
    System.out.println("Done.");
    System.exit(0);
  }
}
