package io.derecho.test;

import io.derecho.Group;
import io.derecho.IMessageGenerator;
import io.derecho.IShardViewGenerator;
import io.derecho.QueryResults;
import io.derecho.Replicated;
import io.derecho.View;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** [TypedSubgroupBWTest] tests the bandwidth to send typed messages through Derecho Java. */
public class TypedSubgroupBWTest
    implements IShardViewGenerator, Group.ICallbackSet, IMessageGenerator {

  // Load derecho JNI library.
  static {
    try {
      System.loadLibrary("derechojni");
    } catch (UnsatisfiedLinkError e) {
      System.out.println(e.getMessage());
    }
  }

  AtomicBoolean is_done;
  int counter;
  int num_nodes;
  int count;
  int message_size;

  // java derecho need a default constructor.
  TypedSubgroupBWTest() {}

  /**
   * The official constructor of TypedSubgroupBWTest.
   *
   * @param number_of_nodes the number of nodes to join the BandwidthTest.
   * @param count number of messages to send through the test. not used in this test.
   * @param ms maximum size of one message to send through the test. not used in this test.
   */
  TypedSubgroupBWTest(int number_of_nodes, int cnt, int ms) {
    this.num_nodes = number_of_nodes;
    this.count = cnt;
    this.message_size = ms;
    is_done = new AtomicBoolean(false);
    counter = 0;
  }

  /** Writes a default message to the buffer so that there could be something to be sent. */
  @Override
  public void write(ByteBuffer buffer) {
    String msg = "Some Messages.";
    buffer.put(msg.getBytes());
  }

  /** Increase the counter once a message is received. */
  @Override
  public void global_stability_callback(
      int subgroup_id, int node_id, long message_id, long version, ByteBuffer data) {

    counter++;
  }

  @Override
  public void local_persistence_callback(int subgroup_id, long version) {
    return;
  }

  @Override
  public void global_persistence_callback(int sugroup_id, long version) {
    return;
  }

  /**
   * Generate the shard view here. In typed bandwidth test, all nodes are in one shard, one
   * subgroup, and one group. The class of this group is TestObject.
   */
  @Override
  public Map<Class<?>, List<List<Set<Integer>>>> generate(
      List<Class<?>> subgroupTypes, View previousView, View currentView) {

    if (currentView.members.size() < num_nodes) return null;

    Map<Class<?>, List<List<Set<Integer>>>> retMap = new HashMap<>();

    Set<Integer> shard = new HashSet<Integer>();

    for (int i = 0; i < this.num_nodes; i++) {
      shard.add(currentView.members.get(i));
    }

    List<Set<Integer>> subgroup = new ArrayList<>();
    subgroup.add(shard);

    List<List<Set<Integer>>> retList = new ArrayList<>();
    retList.add(subgroup);

    retMap.put(TestObject.class, retList);
    return retMap;
  }

  /** Execute the typed bandwidth test. */
  public static void main(String... args) throws NoSuchMethodException {

    if (args.length != 3) {
      System.out.println(
          "Usage: java -jar "
              + TypedSubgroupBWTest.class.getName()
              + ".jar num_nodes, num_messages, message_size");
      return;
    }

    // Arguments passed from command line.
    int number_of_nodes = Integer.parseInt(args[0]);
    int count = Integer.parseInt(args[1]);
    int message_size = Integer.parseInt(args[2]) - 1000;

    // Initialize typed subgroup bandwidth test and group.
    List<Class<?>> subgroupTypes = new ArrayList<Class<?>>();
    subgroupTypes.add(TestObject.class);

    TypedSubgroupBWTest typedSubgroupBWTest =
        new TypedSubgroupBWTest(number_of_nodes, count, message_size);

    Group group = new Group(subgroupTypes, typedSubgroupBWTest, typedSubgroupBWTest);

    int node_rank = group.get_my_rank();

    Replicated testObject = group.getSubgroup(TestObject.class);

    // Get methods and parameters from TestObject
    byte[] bbuf = new byte[message_size];

    Method m1 = TestObject.class.getMethod("bytes_fun", byte[].class);
    Method m2 = TestObject.class.getMethod("finishing_call", int.class);

    // Start to send and get results.
    long start_time = System.nanoTime();

    for (int i = 0; i < count; i++) {
      testObject.ordered_send(m1, bbuf);
    }

    if (node_rank == 0) {
      QueryResults results = testObject.ordered_send(m2, 0);
    }

    long end_time = System.nanoTime();

    // Measure the end time and the bandwidth.
    long nanoseconds_elapsed = (end_time - start_time);
    double msec = (double) nanoseconds_elapsed / 1000000;
    double thp_gbps = ((double) count * message_size * 8) / nanoseconds_elapsed;
    double thp_ops = ((double) count * 1000000000) / nanoseconds_elapsed;

    System.out.println("timespan:\t" + msec + " millisecond.");
    System.out.println("throughput:\t" + thp_gbps + " Gbit/s.");
    System.out.println("throughput:\t" + thp_ops + " ops.");

    while (true) {
      // Reaching an infinite loop so that we do not exit the program.
    }
  }
}

/**
 * The stub object used in typed subgroup bandwidth test. It has various toy functions, including
 * one involving a byte[] object.
 */
class TestObject {

  public void fun(String words) {}

  public void bytes_fun(byte[] bytes) {}

  public boolean finishing_call(int x) {
    return true;
  }
}
