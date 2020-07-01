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

/**
 * [TypedSubgroupTest] tests whether typed messages can be sent correctly and confined in a Derecho
 * Group where all objects are of the same class.
 */
public class TypedSubgroupTest
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

  // The constructor of TypedSubgroupTest does not need any arguments.
  TypedSubgroupTest() {
    is_done = new AtomicBoolean(false);
    counter = 0;
  }

  @Override
  public void global_stability_callback(
      int subgroup_id, int node_id, long message_id, long version, ByteBuffer data) {
    counter++;
  }

  @Override
  public void local_persistence_callback(int subgroup_id, long version) {}

  @Override
  public void global_persistence_callback(int subgroup_id, long version) {}

  /**
   * Generate the shard view here. In typed subgroup test, there are three subgroups. Subgroup
   * [Cache] has one set of three nodes, and subgroup [Foo] and [Bar] shares the other set of three
   * nodes. Each shard contains three nodes in this test.
   */
  @Override
  public Map<Class<?>, List<List<Set<Integer>>>> generate(
      List<Class<?>> subgroupTypes, View previousView, View currentView) {

    System.out.println("subgroup classes" + subgroupTypes);

    int num_members = currentView.members.size();

    Map<Class<?>, List<List<Set<Integer>>>> retMap = new HashMap<>();

    for (Class<?> subgroupClass : subgroupTypes) {

      Set<Integer> shard = new HashSet<>();

      if (subgroupClass.equals(Foo.class) || subgroupClass.equals(Bar.class)) {

        if (num_members < 3) return null;

        for (int i = 0; i < 3; ++i) {
          shard.add(currentView.members.get(i));
        }

      } else if (subgroupClass.equals(Cache.class)) {

        if (num_members < 6) return null;

        for (int i = 3; i < 6; ++i) {
          shard.add(currentView.members.get(i));
        }
      }

      List<Set<Integer>> subgroup = new ArrayList<>();
      subgroup.add(shard);

      List<List<Set<Integer>>> retList = new ArrayList<>();
      retList.add(subgroup);
      retMap.put(subgroupClass, retList);
    }

    System.out.println("retmap :" + retMap);

    return retMap;
  }

  /** Writes some messages. This is not used because we are not using send() here. */
  @Override
  public void write(ByteBuffer buffer) {
    String msg = "Some Messages.";
    buffer.put(msg.getBytes());
  }

  /** Executes the typed subgroup test! */
  public static final void main(String[] args) {

    // No arguments needed.

    List<Class<?>> subgroupTypes = new ArrayList<Class<?>>();
    subgroupTypes.add(Foo.class);
    subgroupTypes.add(Bar.class);
    subgroupTypes.add(Cache.class);

    // Initialize typed subgroup test and group.

    TypedSubgroupTest typedSubgroupTest = new TypedSubgroupTest();

    Group group = new Group(subgroupTypes, typedSubgroupTest, typedSubgroupTest);

    int node_rank = group.get_my_rank();

    Map<Integer, Object> replyMap;
    QueryResults qr;

    try {

      switch (node_rank) {
        case 0:

          // Tests whether we can send objects using ordered_send().

          Replicated fooHandle = group.getSubgroup(Foo.class);
          Replicated barHandle = group.getSubgroup(Bar.class);

          System.out.println("Appending to Bar");

          Method m = Bar.class.getMethod("append", String.class);

          barHandle.ordered_send(m, "Writing from 0...");

          System.out.println("Reading Foo's state just to allow node 1's message to be delivered");

          Method n = Foo.class.getMethod("readState");

          fooHandle.ordered_send(n);

          break;
        case 1:

          // Test whether we can receive messages from ordered_send()
          fooHandle = group.getSubgroup(Foo.class);
          barHandle = group.getSubgroup(Bar.class);

          int new_value = 3;
          System.out.println("Changing Foo's state to " + new_value);

          m = Foo.class.getMethod("changeState", int.class);

          qr = fooHandle.ordered_send(m, new_value);

          replyMap = qr.get();

          for (Integer i : replyMap.keySet()) {

            System.out.println("Reply from node " + i + " was " + replyMap.get(i));
          }

          qr = fooHandle.ordered_send(m, new_value);

          replyMap = qr.get();

          for (Integer i : replyMap.keySet()) {

            System.out.println("Reply from node " + i + " was " + replyMap.get(i));
          }

          System.out.println("Appending to Bar");

          n = Bar.class.getMethod("append", String.class);

          barHandle.ordered_send(n, "Write from 1...");

          break;
        case 2:
          // Tests whether we can send and receive messages from ordered_send().
          fooHandle = group.getSubgroup(Foo.class);
          barHandle = group.getSubgroup(Bar.class);

          System.out.println("Reading Foo's state from the group");

          m = Foo.class.getMethod("readState");

          qr = fooHandle.ordered_send(m);

          replyMap = qr.get();

          for (Integer i : replyMap.keySet()) {

            System.out.println("Node " + i + " says the state is " + replyMap.get(i));
          }

          n = Bar.class.getMethod("append", String.class);

          barHandle.ordered_send(n, "Write from 2...");

          System.out.println("Print log from bar...");

          n = Bar.class.getMethod("print");

          qr = barHandle.ordered_send(n);
          replyMap = qr.get();

          for (Integer i : replyMap.keySet()) {

            System.out.println("Node " + i + " says the log is " + replyMap.get(i));
          }

          System.out.println("Clearing bar's log...");

          n = Bar.class.getMethod("clear");

          barHandle.ordered_send(n);

          break;
        case 3:

          // Test whether we can send more complex messages through ordered_send().

          Replicated cacheHandle = group.getSubgroup(Cache.class);

          System.out.println("Waiting for a 'Ken' value to appear in the cache...");

          boolean found = false;

          while (!found) {

            m = Cache.class.getMethod("contains", String.class);

            qr = cacheHandle.ordered_send(m, "Ken");
            replyMap = qr.get();

            boolean contains_accum = true;

            for (Integer i : replyMap.keySet()) {

              boolean contains_result = (boolean) replyMap.get(i);

              System.out.println("Reply from node " + i + ": " + contains_result);

              contains_accum &= contains_result;
            }

            found = contains_accum;
          }

          System.out.println("Found!...");

          m = Cache.class.getMethod("get", String.class);

          qr = cacheHandle.ordered_send(m, "Ken");
          replyMap = qr.get();

          for (Integer i : replyMap.keySet()) {

            System.out.println("Node " + i + " had Ken = " + replyMap.get(i));
          }

          break;
        case 4:

          // Test whether we can go through p2p_send().

          cacheHandle = group.getSubgroup(Cache.class);

          System.out.println("Putting Ken = Birman into the cache...");

          m = Cache.class.getMethod("put", String.class, String.class);

          cacheHandle.ordered_send(m, "Ken", "Birman");

          cacheHandle.ordered_send(m, "Ken", "Birman");

          // this ASSUMES that node id 0 belongs to the Foo and Bar subgroup.

          // this might not be the case.

          int p2p_target = 0;

          System.out.println("Reading Foo's state from node " + p2p_target);

          m = Foo.class.getMethod("changeState", int.class);

          cacheHandle.p2p_send(Foo.class, p2p_target, m, 5);

          m = Foo.class.getMethod("readState");

          qr = cacheHandle.p2p_send(Foo.class, p2p_target, m);

          replyMap = qr.get();

          Object response = replyMap.get(p2p_target);

          System.out.println("Response: " + response);

          break;
        case 5:

          // Test whether ordered_send would refresh the cache.

          cacheHandle = group.getSubgroup(Cache.class);

          System.out.println("Putting Ken = Woodberry into the cache...");

          m = Cache.class.getMethod("put", String.class, String.class);

          cacheHandle.ordered_send(m, "Ken", "Woodberry");

          cacheHandle.ordered_send(m, "Ken", "Woodberry");

          break;
        default:
          break;
      }
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
      System.out.println("No such method! Leaving group...");
      group.barrierSync();
      group.leave();
      System.out.println("Done.");
      System.exit(0);
      return;
    }

    System.out.println("reaching end of main, entering an infinite loop");

    while (true) {}
  }
}

/** Stub objects and classes used for testing typed subgroup correctness. */
class Foo {

  int state;

  public Foo() {
    state = 0;
  }

  public int readState() {

    return state;
  }

  public boolean changeState(int newState) {

    if (newState == state) {

      return false;
    }

    state = newState;

    return true;
  }

  Foo(int initialState) {

    this.state = initialState;
  }
}

/** Stub objects and classes used for testing typed subgroup correctness. */
class Bar {

  String log;

  public Bar() {
    log = "";
  }

  public Bar(String str) {

    log = str;
  }

  public void append(String words) {
    System.out.println("calling append!");
    log += words;
    System.out.println(log);
  }

  public void clear() {
    log = "";
  }

  public String print() {
    return log;
  }
}

/** Stub objects and classes used for testing typed subgroup correctness. */
class Cache {

  Map<String, String> cacheMap;

  public Cache() {
    cacheMap = new HashMap<>();
  }

  public Cache(Map<String, String> map) {
    cacheMap = map;
  }

  public void put(String key, String value) {

    cacheMap.put(key, value);
  }

  public String get(String key) {

    return cacheMap.get(key);
  }

  public boolean contains(String key) {

    return cacheMap.containsKey(key);
  }

  public boolean invalidate(String key) {

    if (cacheMap.get(key) == null) return false;

    cacheMap.remove(key);

    return true;
  }
}
