#pragma once

#ifndef MINCOSTFLOW_HPP
#define MINCOSTFLOW_HPP

#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <queue>
#include <derecho/core/derecho_type_definitions.hpp>
#include <derecho/core/detail/derecho_internal.hpp>

namespace mincostflow {
using vertex_id_t = std::pair<int, int>;

enum vertex_type_t {
    source, sink, shard, fcs
};

struct Vertex {
    vertex_type_t type;
    vertex_id_t id;

    bool operator==(const Vertex& other) const;
    bool operator!=(const Vertex& other) const;
};

constexpr static const Vertex source_vertex = Vertex{source, {-1,-1}};
constexpr static const Vertex sink_vertex = Vertex{sink, {-1,-1}};
}

namespace std {
using namespace mincostflow;

template<>
struct hash<vertex_id_t> {
    std::size_t operator()(const vertex_id_t& v) const {
        return v.first + 31 * v.second;
    }
};
template <>
struct hash<Vertex> {
    std::size_t operator()(const Vertex& v) const {
        hash<vertex_id_t> id_hasher;
        return size_t(v.type) + id_hasher(v.id) * 31;
    }
};
template<>
struct hash<std::pair<Vertex, Vertex>> {
    std::size_t operator()(const std::pair<Vertex, Vertex>& v) const {
        hash<Vertex> vertex_hasher;
        return vertex_hasher(v.first) + vertex_hasher(v.second) * 31;
    }
};

}

namespace mincostflow {
struct Edge {
    Vertex head;
    Vertex tail;
    uint32_t cost;
    uint32_t capacity;
    uint32_t flow;

    const Vertex residual_head() const;
    const Vertex residual_tail() const;
    uint32_t residual_capacity() const;
    bool residual_has_same_dir() const;
};

struct Path {
    uint32_t cost;
    uint32_t capacity;
    std::vector<Edge> edges;
    std::unordered_set<Vertex> used_vertices;

    Path() : cost(0), capacity(UINT32_MAX), edges({}), used_vertices({}) {}
    bool operator<(const Path& other) const;
    bool add(Edge edge);
    const Vertex tail() const;
};

struct Shard {
    vertex_id_t shard_id;
    uint32_t min_fcs;
    mutable std::unordered_set<node_id_t> members;
    bool operator==(const Shard& other) const;
    bool operator!=(const Shard& other) const;
};

}

namespace std {
using namespace mincostflow;

template<>
struct hash<Shard> {
    std::size_t operator()(const Shard& s) const {
        hash<vertex_id_t> id_hasher;
        return id_hasher(s.shard_id);
    }
};
}

namespace mincostflow {
class Graph {
public:
    Graph(std::unordered_map<std::pair<Vertex, Vertex>, Edge>& edges,
          std::unordered_set<Vertex>& shard_vertices,
          std::unordered_set<Vertex>& fcs_vertices);
    uint32_t get_flow();
    void add_flow();
    Edge get_edge(Vertex head, Vertex tail);
private:
    std::unordered_map<std::pair<Vertex, Vertex>, Edge> edges;
    std::unordered_set<Vertex> shard_vertices;
    std::unordered_set<Vertex> fcs_vertices;
    uint32_t flow;
    std::priority_queue<Path> create_paths_from_source();
    void add_paths_from_shard_to_fcs(Vertex shard, Path path,
            std::priority_queue<Path>& paths);
    void add_paths_from_fcs_to_shard(Vertex fcs, Path path,
            std::priority_queue<Path>& paths);
    void add_path(Vertex shard, Vertex fcs, Path path,
            std::priority_queue<Path>& paths);
    std::optional<Path> path_to_sink(Vertex fcs, Path path);
    void augment(Path augmenting_path);
};

class MinCostFlowSolver {
public:
    MinCostFlowSolver(Graph graph, uint32_t target_flow);
    void solve();
    Graph get_graph();
private:
    Graph graph;
    uint32_t target_flow;
};

class FcsAllocator{
public:
    FcsAllocator(std::unordered_set<Shard>& shards,
            std::unordered_map<node_id_t, derecho::fcs_id_t>& node_to_fcs,
            std::unordered_map<derecho::fcs_id_t, std::unordered_set<node_id_t>>& fcs_to_nodes);
    std::unordered_set<Shard> allocate();
private:
    std::unordered_set<Shard> shards;
    std::unordered_map<node_id_t, derecho::fcs_id_t> node_to_fcs;
    std::unordered_map<derecho::fcs_id_t, std::unordered_set<node_id_t>> fcs_to_nodes;
    std::pair<Graph, uint32_t> to_graph_and_target_flow();
    std::unordered_set<Shard> from_graph(Graph graph);
};

}

#endif //MINCOSTFLOW_HPP
