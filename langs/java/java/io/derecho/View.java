package io.derecho;

import java.util.ArrayList;
import java.util.List;

/** The current view of the Java group. */
public class View {
  /** Sequential view ID: 0, 1, 2, ... */
  public int viewId;
  /** Node IDs of members in the current view, index by their SST rank. */
  public List<Integer> members;
  /** Rank of this node */
  public int myRank;
  /**
   * Set to false during MulticastGroup setup if a subgroup membership function throws a
   * subgroup_provisioning_exception. If false, no subgroup operations will work in this View.
   */
  public boolean isAdequatelyProvisioned;
  /** TODO unimplemented now */
  public List<Boolean> failed;
  /** TODO unimplemented now */
  public int numFailed;

  /**
   * subgroup ids for each subgroup, in the order of Class<?> list provided by user-defined behavior
   * of Group::IShardViewGenerator
   */
  // public List<Integer> subgroupIDs;
  // TODO: more members can be borrowed from the c++ derecho::View implementation.

  /** Constructor of the View class. */
  public View() {
    members = new ArrayList<Integer>();
    failed = new ArrayList<Boolean>();
  }
}
