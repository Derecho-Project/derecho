package io.derecho.test;

import io.derecho.Group;
import io.derecho.IMessageGenerator;
import io.derecho.IShardViewGenerator;
import io.derecho.Replicated;
import io.derecho.View;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** [LatencyTest] tests the average latency to send one raw message through Derecho Java. */
public class LatencyTest implements IShardViewGenerator, Group.ICallbackSet {

  // Load derecho JNI library.
  static {
    try {
      System.loadLibrary("derechojni");
    } catch (UnsatisfiedLinkError e) {
      System.out.println(e.getMessage());
    }
  }

  int num_nodes;
  int delivery_mode; // TODO: not supported right now. feature to be added later.
  int num_senders_selector;
  int num_messages;
  AtomicBoolean is_done;

  int my_id;
  int counter = 0;
  long[] start_time;
  long[] end_time;

  /** java derecho need a default constructor. */
  LatencyTest() {}

  /**
   * The official constructor of LatencyTest.
   *
   * @param number_of_nodes the number of nodes to join the LatencyTest.
   * @param mode Usually 0 while we are testing. Not used. (ordered send)
   * @param selector 0: all senders. 1: half senders. 2: one sender.
   * @param number_of_message Number of messages to send throughout the test.
   */
  LatencyTest(int number_of_nodes, int mode, int selector, int number_of_message) {
    this.start_time = new long[number_of_message];
    this.end_time = new long[number_of_message];
    this.num_nodes = number_of_nodes;
    this.delivery_mode = mode;
    this.num_senders_selector = selector;
    this.num_messages = number_of_message;
    is_done = new AtomicBoolean(false);
  }

  /**
   * After work is done, Java side would call this function to notify Java side that the work is
   * finished. Java side should also record the end time of sending messages here to calculate
   * latency later.
   */
  @Override
  public void global_stability_callback(
      int subgroup_id, int node_id, long message_id, long version, ByteBuffer data) {

    long j = System.nanoTime();
    counter++;
    int i = data.getInt();
    if (node_id == my_id) {

      end_time[i] = j;
    }

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
   * Generate the shard view here. In latency test, all nodes are in one shard, one subgroup, and
   * one group. The class of this group is LatencyTest.
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
    retMap.put(LatencyTest.class, retList);
    return retMap;
  }

  /** Execute the latency test. */
  public static final void main(String[] args) {
    if (args.length != 5) {
      System.out.println(
          "Usage: java -jar "
              + LatencyTest.class.getName()
              + ".jar num_nodes, num_senders_selector (0 - all senders, 1 - half senders, 2 - one sender), num_messages, delivery_mode (0 - ordered mode, 1 - unordered mode), message_size");
      return;
    }

    // Arguments passed from command line.
    int number_of_nodes = Integer.parseInt(args[0]);
    int num_senders_selector = Integer.parseInt(args[1]);
    int num_messages = Integer.parseInt(args[2]);
    int delivery_mode = Integer.parseInt(args[3]);
    int message_size = Integer.parseInt(args[4]);

    // Initialize latency test and group.
    List<Class<?>> subgroupTypes = new ArrayList<Class<?>>();
    subgroupTypes.add(LatencyTest.class);
    LatencyTest latencyTest =
        new LatencyTest(number_of_nodes, delivery_mode, num_senders_selector, num_messages);
    Group group = new Group(subgroupTypes, latencyTest, latencyTest);
    List<Integer> members_order = group.getMembers();
    int node_rank = group.get_my_rank();
    latencyTest.my_id = members_order.get(node_rank);
    System.out.println("Finished joining group with rank = " + node_rank);

    // Send message.

    Replicated raw_subgroup = group.getSubgroup(LatencyTest.class, 0);
    boolean is_send =
        num_senders_selector == 0
            || (num_senders_selector == 1 && node_rank > (number_of_nodes - 1) / 2)
            || (num_senders_selector == 2 && node_rank == number_of_nodes - 1);

    // Start Timer
    if (is_send) {
      for (int i = 0; i < num_messages; i++) {
        raw_subgroup.send(message_size, new LatencyTestWriter(latencyTest, i));
      }
    } else {
      // do nothing
    }

    // wait until everybody in the group is done
    try {
      while (!latencyTest.is_done.get()) {}

    } catch (Exception e) {
      e.printStackTrace();
      // do nothing
    }
    // End Timer

    // calculate latency measured for every single message sent.

    String str = "";

    if (is_send) {
      double total_time = 0;
      for (int i = 0; i < num_messages; ++i) {
        total_time += latencyTest.end_time[i] - latencyTest.start_time[i];
      }
      double avg_time = total_time / num_messages;
      double sum_of_square = 0;
      for (int i = 0; i < num_messages; ++i) {
        sum_of_square +=
            (latencyTest.end_time[i] - latencyTest.start_time[i] - avg_time)
                * (latencyTest.end_time[i] - latencyTest.start_time[i] - avg_time);
      }
      double std_dev = Math.sqrt(sum_of_square / (num_messages - 1));

      System.out.println(
          "As a node with rank "
              + node_rank
              + ", my latency is "
              + avg_time
              + " ns with stddev "
              + std_dev
              + " ns.");

      str = node_rank + ", " + avg_time + ", " + std_dev;

    } else str = "I am not a sender this time.";

    // writes the results to out.log file.
    try {
      final Path path = Paths.get("out.log");
      Files.write(
          path,
          Arrays.asList(str),
          StandardCharsets.UTF_8,
          Files.exists(path) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Leaves the group.
    System.out.println("Leaving group...");
    group.barrierSync();
    group.leave();
    System.out.println("Done.");
    System.exit(0);
  }
}

/**
 * The writer to write messages in Latency Test. To make results accurate, we would write down the
 * message number as the message and pass it through the Latency Test.
 */
class LatencyTestWriter implements IMessageGenerator {
  LatencyTest lt;
  int i;

  LatencyTestWriter(LatencyTest lt, int i) {
    this.lt = lt;
    this.i = i;
  }

  /** Writes the message number to the buffer and passes it through latency test. */
  @Override
  public void write(ByteBuffer buffer) {
    // System.out.println(i);
    buffer.putInt(i);

    lt.start_time[i] = System.nanoTime();
  }
}
