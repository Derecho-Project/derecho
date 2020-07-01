package io.derecho;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * View Generator, user application should provide an implementation to generate shard view of the
 * Java group.
 */
public interface IShardViewGenerator {
  /**
   * Shard View Generator generates the view based on the view we get.
   *
   * @param subgroupTypes The list of Java subgroup classes in the current group.
   * @param previousView The previous view.
   * @param currentView The current view.
   * @return A map of the view layout. [null] for failing to generate a shard view.
   */
  public Map<Class<?>, List<List<Set<Integer>>>> generate(
      List<Class<?>> subgroupTypes, View previousView, View currentView);
}
