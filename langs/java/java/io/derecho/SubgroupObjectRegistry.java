package io.derecho;

import java.util.HashMap;
import java.util.Map;

/**
 * The registry that registers each Java object with the subgroup ID that it belongs to in C++ side.
 */
public class SubgroupObjectRegistry {
  /** static variable single instance of the Registry */
  private static SubgroupObjectRegistry sor;

  /** Mapping of subgroup_type_id to java type */
  public Map<Integer, Object> sor_map;

  /** private constructor restricted to this class itself */
  private SubgroupObjectRegistry() {
    sor_map = new HashMap<Integer, Object>();
  }

  /**
   * Adds the subgroup ID - object pair to the mapping.
   *
   * @param sid the Subgroup ID that [obj] belongs to in C++ group.
   * @param obj The object to register in the registry.
   */
  public void addSidTypePair(int sid, Object obj) {
    this.sor_map.put(sid, obj);
  }

  /**
   * Get the object with subgroup ID [sid].
   *
   * @param sid The subgroup ID to get object.
   * @return The object with subgroup ID [sid].
   */
  Object getObj(int sid) {
    return sor_map.get(sid);
  }

  /**
   * static method to get instance of SubgroupObjectRegistry class
   *
   * @return an instance of subgroup object registry corresponding to the current group.
   */
  public static SubgroupObjectRegistry getInstance() {
    return sor;
  }

  /** Creates a new subgroup object registry each time initializing. */
  static {
    sor = new SubgroupObjectRegistry();
  }
}
