#include <derecho/mincostflow/mincostflow.hpp>
#include <derecho/core/derecho_exception.hpp>

using namespace mincostflow;
namespace mincostflow {
    bool Vertex::operator==(const Vertex &other) const {
        return type == other.type && id == other.id;
    }
    bool Vertex::operator!=(const Vertex &other) const {
        return !(*this == other);
    }

    bool Edge::residual_has_same_dir() const {
        return capacity > flow;
    }
    uint32_t Edge::residual_capacity() const {
        return residual_has_same_dir() ? capacity - flow : capacity;
    }
    const Vertex Edge::residual_head() const {
        return residual_has_same_dir() ? head : tail;
    }
    const Vertex Edge::residual_tail() const {
        return residual_has_same_dir() ? tail : head;
    }

    bool Shard::operator==(const Shard& other) const {
        return shard_id == other.shard_id;
    }
    bool Shard::operator!=(const Shard& other) const {
        return !(*this == other);
    }


    bool Path::operator<(const Path& other) const {
        //want lower cost to rank higher
        return cost > other.cost;
    }
    const Vertex Path::tail() const {
        return edges.back().residual_tail();
    }
    bool Path::add(const Edge edge) {
        const Vertex& edge_head = edge.residual_head();
        const Vertex& edge_tail = edge.residual_tail();
        // not starting from the last vertex of this path
        if (!edges.empty() && edge_head != tail())
            return false;
        // comes from sink or goes to source
        if (edge_head.type == sink || edge_tail.type == source)
            return false;
        // goes back to a vertex we've visited
        auto inserted = used_vertices.insert(edge_tail);
        if (!inserted.second)
            return false;
        used_vertices.insert(edge_head);

        edges.push_back(edge);
        cost += edge.cost;
        capacity = std::min(edge.residual_capacity(), capacity);
        return capacity > 0;
    }

    Graph::Graph(std::unordered_map<std::pair<Vertex, Vertex>, Edge>& edges,
        std::unordered_set<Vertex>& shard_vertices,
        std::unordered_set<Vertex>& fcs_vertices) :
            edges(edges), shard_vertices(shard_vertices), fcs_vertices(fcs_vertices), flow(0) {}
    uint32_t Graph::get_flow() {
        return flow;
    }
    void Graph::add_flow() {
        std::priority_queue<Path> paths = create_paths_from_source();

        std::optional<Path> augmenting_path = std::nullopt;
        while (!augmenting_path.has_value()) {
            if (paths.empty()) {
                throw derecho::fcs_allocation_exception("No augmenting paths possible to add flow");
            }

            const Path path = paths.top();
            paths.pop();
            const Vertex& tail = path.tail();
            switch (tail.type) {
                case source:
                case sink:
                    throw derecho::fcs_allocation_exception("Augmenting path ends prematurely");
                case shard:
                    add_paths_from_shard_to_fcs(tail, path, paths);
                    break;
                case fcs:
                    augmenting_path = path_to_sink(tail, path);
                    if (augmenting_path.has_value()) {
                        break;
                    }
                    add_paths_from_fcs_to_shard(tail, path, paths);
            }
        }

        augment(augmenting_path.value());
    }
    Edge Graph::get_edge(Vertex head, Vertex tail) {
        return edges.at({head, tail});
    }
    std::priority_queue<Path> Graph::create_paths_from_source() {
        std::priority_queue<Path> paths = {};
        for (const Vertex& shard : shard_vertices) {
            Edge edge = edges.at({source_vertex, shard});
            Path source_to_shard;
            if (source_to_shard.add(edge))
                paths.push(source_to_shard);
        }
        return paths;
    }
    void Graph::add_paths_from_shard_to_fcs(Vertex shard, Path path,
            std::priority_queue<Path>& paths) {
        for (const Vertex& fcs : fcs_vertices) {
            add_path(shard, fcs, path, paths);
        }
    }
    void Graph::add_paths_from_fcs_to_shard(Vertex fcs, Path path,
            std::priority_queue<Path>& paths) {
        for (const Vertex& shard : shard_vertices) {
            add_path(shard, fcs, path, paths);
        }
    }
    void Graph::add_path(Vertex shard, Vertex fcs, Path path,
                        std::priority_queue<Path>& paths) {
        Edge edge = edges.at({shard, fcs});
        Path shard_to_fcs = path;
        if (shard_to_fcs.add(edge))
            paths.push(shard_to_fcs);
    }
    std::optional<Path> Graph::path_to_sink(Vertex fcs, Path path) {
        Edge edge = edges.at({fcs, sink_vertex});
        Path path_to_sink = path;
        return path_to_sink.add(edge) ? std::optional<Path>(path_to_sink) : std::nullopt;
    }
    void Graph::augment(Path augmenting_path) {
        for (const Edge& residualEdge : augmenting_path.edges) {
            Edge& edge = edges[{residualEdge.head, residualEdge.tail}];
            if (edge.residual_has_same_dir()) {
                edge.flow += augmenting_path.capacity;
            } else {
                edge.flow -= augmenting_path.capacity;
            }
        }
        flow += augmenting_path.capacity;
    }


    MinCostFlowSolver::MinCostFlowSolver(Graph graph, uint32_t target_flow) :
        graph(graph), target_flow(target_flow) {}
    void MinCostFlowSolver::solve() {
        while (graph.get_flow() < target_flow) {
            graph.add_flow();
        }
    }
    Graph MinCostFlowSolver::get_graph() {
        return graph;
    }


    FcsAllocator::FcsAllocator(std::unordered_set<Shard>& shards,
            std::unordered_map<node_id_t, derecho::fcs_id_t>& node_to_fcs,
            std::unordered_map<derecho::fcs_id_t, std::unordered_set<node_id_t>>& fcs_to_nodes) :
            shards(shards), node_to_fcs(node_to_fcs), fcs_to_nodes(fcs_to_nodes) {}
    std::unordered_set<Shard> FcsAllocator::allocate() {
        auto [graph, target_flow] = to_graph_and_target_flow();
        MinCostFlowSolver solver(graph, target_flow);
        solver.solve();
        return from_graph(solver.get_graph());
    }
    std::pair<Graph, uint32_t> FcsAllocator::to_graph_and_target_flow() {
        uint32_t target_flow = 0;
        std::unordered_map<vertex_id_t, Vertex> shard_map(shards.size());
        std::unordered_set<Vertex> shard_set(shards.size());
        for (const Shard& s : shards) {
            const Vertex& shard_vertex = {shard, s.shard_id};
            shard_map[s.shard_id] = shard_vertex;
            shard_set.insert(shard_vertex);
            target_flow += s.min_fcs;
        }
        std::unordered_map<vertex_id_t, Vertex> fcs_map(fcs_to_nodes.size());
        std::unordered_set<Vertex> fcs_set(fcs_to_nodes.size());
        for (const auto& fcs_and_nodes : fcs_to_nodes) {
            const Vertex& fcs_vertex = {fcs, {fcs_and_nodes.first, 0}};
            fcs_map[{fcs_and_nodes.first, 0}] = fcs_vertex;
            fcs_set.insert(fcs_vertex);
        }

        std::unordered_map<std::pair<Vertex, Vertex>, Edge> edges(shards.size() * fcs_to_nodes.size());
        // edges from source to shard
        for (const Shard& s : shards) {
            const Vertex& shard_vertex = shard_map[s.shard_id];
            edges[{source_vertex, shard_vertex}] = {source_vertex, shard_vertex, 0, s.min_fcs, 0};
        }
        // edges from shard to FCS
        for (const Shard& s : shards) {
            const Vertex& shard_vertex = shard_map[s.shard_id];
            std::unordered_set<int> fcs_contained(s.members.size());
            for (node_id_t node : s.members) {
                fcs_contained.insert(node_to_fcs[node]);
            }

            for (const auto& fcs_and_nodes : fcs_to_nodes) {
                const Vertex& fcs_vertex = fcs_map[{fcs_and_nodes.first, 0}];
                uint32_t cost = fcs_contained.find(fcs_and_nodes.first) == fcs_contained.end() ?
                        1 : 0;
                edges[{shard_vertex, fcs_vertex}] = {shard_vertex, fcs_vertex, cost, 1, 0};
            }
        }
        // edges from FCS to sink
        for (const auto& [fcs_id, nodes] : fcs_to_nodes) {
            const Vertex& fcs_vertex = fcs_map[{fcs_id, 0}];
            edges[{fcs_vertex, sink_vertex}] = {fcs_vertex, sink_vertex, 0, uint32_t(nodes.size()), 0};
        }

        return {{edges, shard_set, fcs_set}, target_flow};
    }
    std::unordered_set<Shard> FcsAllocator::from_graph(Graph graph) {
        std::unordered_set<Shard> new_shards;
        //1. Assign nodes that remain
        for (const Shard& s : shards) {
            std::unordered_set<node_id_t> new_assignment(s.min_fcs);
            for (auto& [fcs_id, nodes] : fcs_to_nodes) {
                Edge edge = graph.get_edge({shard, s.shard_id}, {fcs, {fcs_id, 0}});
                if (edge.flow != 1 || edge.cost != 0)
                    continue;

                std::optional<node_id_t> remaining_node = std::nullopt;
                for (node_id_t node : s.members) {
                    if (node_to_fcs[node] == fcs_id) {
                        remaining_node.emplace(node);
                        break;
                    }
                }
                if (!remaining_node.has_value()) {
                    throw derecho::fcs_allocation_exception("Could not find remaining node");
                }
                nodes.erase(remaining_node.value());
                new_assignment.insert(remaining_node.value());
            }
            new_shards.insert({s.shard_id, s.min_fcs, new_assignment});
        }

        //2. Assign transferred nodes arbitrarily
        for (const Shard& s : new_shards) {
            if (s.members.size() == s.min_fcs)
                continue;

            for (auto& [fcs_id, nodes] : fcs_to_nodes) {
                Edge edge = graph.get_edge({shard, s.shard_id}, {fcs, {fcs_id, 0}});
                if (edge.flow != 1 || edge.cost != 1)
                    continue;

                node_id_t node = *nodes.begin();
                nodes.erase(node);
                s.members.insert(node);
            }
        }
        return new_shards;
    }

}