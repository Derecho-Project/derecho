#ifndef SST_H
#define SST_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "NamedRowPredicates.h"
#include "combinator_utils.h"
#include "combinators.h"
#include "util.h"
#include "verbs.h"

/** The root namespace for the Shared State Table project. */
namespace sst {

using std::vector;
using std::cout;
using std::endl;
using std::function;
using std::list;
using std::pair;
using std::map;
using std::thread;
using std::unique_ptr;

using util::template_and;

/**
 * Flag to determine whether an SST is implemented in reads mode or writes mode.
 */
enum class Mode {
    /** In reads mode, the SST will continuously refresh the local copy of
     * the table by posting one-sided reads to the remote nodes. */
    Reads,
    /** In writes mode, the SST waits for its local copy of the table to be
     * updated by a one-sided write from a remote node, and each node is
     * responsible for calling SST::put() to initiate this write after
     * changing a variable in its local row. */
    Writes
};

typedef function<void(uint32_t)> failure_upcall_t;

/**
 * The SST object, representing a single shared state table.
 *
 * @tparam Row The type of the structure that will be used for each row in
 * this SST
 * @tparam ImplMode A {@link Mode} enum value indicating whether this SST will
 *be
 * implemented in Reads mode or Writes mode; default is Writes.
 * @tparam NameEnum An enum that will be used to name functions, if named
 * functions are used with this SST. Defaults to util::NullEnum, which is empty.
 * @tparam NamedFunctionTypePack A struct containing the function types and
 * return types of all the named functions that will be registered with this
 * SST; defaults to an empty struct if no named functions are used.
 */
template <class Row, Mode ImplMode = Mode::Writes,
          typename NameEnum = util::NullEnum,
          typename RowExtras = NamedRowPredicates<>>
class SST {
    using NamedRowPredicatesTypePack =
        typename RowExtras::NamedRowPredicatesTypePack;

    // Row struct must be POD. In addition, it should not contain any pointer
    // types
    static_assert(std::is_pod<Row>::value, "Error! Row type must be POD.");
    // static_assert(forall_type_list<this should be is_base with the Row!,
    // NamedRowPredicatesTypePack>(),"Error: RowPredicates built on wrong
    // row!");

public:
    struct InternalRow : public Row,
                         public util::extend_tuple_members<
                             typename NamedRowPredicatesTypePack::row_types> {};

private:
    using named_functions_t =
        typename NamedRowPredicatesTypePack::template Getters<
            const volatile InternalRow &>;

    //			   std::declval<util::n_copies<NamedRowPredicatesTypePack::size::value,
    //			   std::function<bool (volatile const InternalRow&,
    // int)>
    //>
    //>()))>;
    /** List of functions we have registered to use knowledge operators on */
    named_functions_t named_functions;

public:
    /**
     * An object containing a read-only snapshot of an SST. It can be used
     * like an SST for reading row values and the states of named functions,
     * but it is disconnected from the actual SST and will not get updated.
     */
    class SST_Snapshot {
    private:
        /** Number of members, which is the number of rows in `table`. */
        int num_members;
        /** The structure containing shared state data. */
        unique_ptr<const InternalRow[]> table;

    public:
        /** Creates an SST snapshot given the current state internals of the
         * SST. */
        SST_Snapshot(const unique_ptr<volatile InternalRow[]> &_table,
                     int _num_members);
        /** Copy constructor. */
        SST_Snapshot(const SST_Snapshot &to_copy);

        /** Accesses a row of the snapshot. */
        const InternalRow &get(int index) const;
        /** Accesses a row of the snapshot using the [] operator. */
        const InternalRow &operator[](int index) const;
    };

private:
    /** List of members in the group (values are node ranks). */
    vector<uint32_t> members;
    /** List of node ranks mapped to member index. */
    map<uint32_t, int, std::greater<uint32_t>> members_by_rank;
    /** Number of members; equal to `members.size()`. */
    unsigned int num_members;
    /** Index of this node in the table. */
    unsigned int member_index;
    /** The actual structure containing shared state data. */
    unique_ptr<volatile InternalRow[]> table;
    /** Map of queue pair number to row. Useful for detecting failures. */
    std::map<int, int> qp_num_to_index;
    /** A parallel array tracking whether the row has been marked frozen. */
    std::vector<bool> row_is_frozen;
    /** The number of rows that have been frozen. */
    int num_frozen{0};
    /** The function to call when a remote node appears to have failed. */
    failure_upcall_t failure_upcall;
    /** List of functions needed to update row predicates */
    using row_predicate_updater_t = std::function<void(SST &)>;
    const std::vector<row_predicate_updater_t>
        row_predicate_updater_functions;  // should be of size
    // NamedPredicatesTypePack::num_updater_functions:::value
    /** RDMA resources vector, one for each member. */
    vector<unique_ptr<resources>> res_vec;
    /** Holds references to background threads, so that we can shut them down
     * during destruction. */
    vector<thread> background_threads;
    /** A flag to signal background threads to shut down; set to true during
     * destructor calls. */
    std::atomic<bool> thread_shutdown;
    /** Indicates whether the predicate evaluation thread should start after
     * being
     * forked in the constructor. */
    bool thread_start;
    /** Mutex for thread_start_cv. */
    std::mutex thread_start_mutex;
    /** Notified when the predicate evaluation thread should start. */
    std::condition_variable thread_start_cv;

    // mutex for put
    std::mutex freeze_mutex;

    /** Base case for the recursive constructor_helper with no template
     * parameters. */
    template <int index>
    auto constructor_helper() const {
        using namespace std;
        return make_pair(tuple<>{}, vector<row_predicate_updater_t>{});
    }

    /** Helper function for the constructor that recursively unpacks Named
     * RowPredicate template parameters. */

    template <int index, typename ExtensionList, typename... RestFunctions>
    auto constructor_helper(const PredicateBuilder<Row, ExtensionList> &pb,
                            const RestFunctions &... rest) const {
        using namespace std;
        using namespace util;
        static_assert(PredicateBuilder<Row, ExtensionList>::is_named::value,
                      "Error: cannot build SST with unnamed predicate");
        static_assert(
            PredicateBuilder<Row, ExtensionList>::template name_enum_matches<
                NameEnum>::value,
            "Error: this predicate contains names from a different NameEnum!");

        constexpr auto num_getters =
            PredicateBuilder<Row, ExtensionList>::num_getters::value;
        auto rec_call_res = constructor_helper<index + num_getters>(rest...);

        std::vector<row_predicate_updater_t> &row_predicate_updater_functions =
            rec_call_res.second;
        predicate_builder::map_updaters(
            row_predicate_updater_functions,
            [](const auto &f, auto const *const RE_type) {
                return [f](SST &sst) -> void {
                    using Row_Extension = decay_t<decltype(*RE_type)>;
                    f(sst.table[sst.get_local_index()], [&](int row) {
                        return ref_pair<volatile Row, volatile Row_Extension>{
                            sst.table[row], sst.table[row]};
                    },
                      sst.get_num_rows());
                };
            },
            pb);

        return make_pair(tuple_cat(pb.template wrap_getters<InternalRow>(),
                                   rec_call_res.first),
                         row_predicate_updater_functions);
    }

    // Functions for background threads to run
    /** Reads all the remote rows once by RDMA, if this SST is in Reads mode. */
    void refresh_table();
    /** Continuously refreshes all the remote rows, if this SST is in Reads
     * mode.
     */
    void read();
    /** Continuously evaluates predicates to detect when they become true. */
    void detect();

public:
    /**
     * Constructs an SST instance, initializes RDMA resources, and spawns
     * background threads.
     *
     * @param _members A vector of node ranks (IDs), each of which represents a
     * node participating in the SST. The order of nodes in this vector is the
     * order in which their rows will appear in the SST.
     * @param my_node_id The node rank of the local node, i.e. the one on which
     * this code is running.
     */
    SST(const vector<uint32_t> &_members, uint32_t my_node_id,
        failure_upcall_t failure_upcall = nullptr, std::vector<char> already_failed = {},
        bool start_predicate_thread = true)
            : SST(_members, my_node_id,
                  std::pair<std::tuple<>, std::vector<row_predicate_updater_t>>{},
                  failure_upcall, already_failed, start_predicate_thread) {}

    template <typename ExtensionList, typename... RestFunctions>
    SST(const vector<uint32_t> &_members, uint32_t my_node_id,
        failure_upcall_t failure_upcall, bool start_predicate_thread,
        std::vector<char> already_failed,
        const PredicateBuilder<Row, ExtensionList> &pb,
        RestFunctions... named_funs)
            : SST(_members, my_node_id, constructor_helper<0>(pb, named_funs...),
                  failure_upcall, already_failed, start_predicate_thread) {}

    template <typename ExtensionList, typename... RestFunctions>
    SST(const vector<uint32_t> &_members, uint32_t my_node_id,
        const PredicateBuilder<Row, ExtensionList> &pb,
        RestFunctions... named_funs)
            : SST(_members, my_node_id, constructor_helper<0>(pb, named_funs...),
                  nullptr, {}, true) {}

    /**
     * Delegate constructor to construct an SST instance without named
     * functions.
     * @param _members A vector of node ranks (IDs), each of which represents a
     * node participating in the SST. The order of nodes in this vector is the
     * order in which their rows will appear in the SST.
     * @param _node_rank The node rank of the local node, i.e. the one on which
     * this code is running.
     */
    SST(const vector<uint32_t> &_members, uint32_t my_node_id,
        std::pair<decltype(named_functions),
                  std::vector<row_predicate_updater_t>>,
        failure_upcall_t _failure_upcall = nullptr,
        std::vector<char> already_failed = {},
        bool start_predicate_thread = true);
    SST(const SST &) = delete;
    virtual ~SST();
    /** Starts the predicate evaluation loop. */
    void start_predicate_evaluation();
    /** Accesses a local or remote row. */
    volatile InternalRow &get(unsigned int index);
    /** Read-only access to a local or remote row, for use in const contexts. */
    const volatile InternalRow &get(unsigned int index) const;
    /** Accesses a local or remote row using the [] operator. */
    volatile InternalRow &operator[](unsigned int index);
    /** Read-only [] operator for a local or remote row, for use in const
     * contexts. */
    const volatile InternalRow &operator[](unsigned int index) const;
    int get_num_rows() const;
    /** Gets the index of the local row in the table. */
    int get_local_index() const;
    /** Gets a snapshot of the table. */
    std::unique_ptr<SST_Snapshot> get_snapshot() const;
    /** Writes the local row to all remote nodes. */
    void put();
    /** Writes the local row to some of the remote nodes. */
    void put(vector<uint32_t> receiver_ranks);
    /** Writes a contiguous subset of the local row to all remote nodes. */
    void put(long long int offset, long long int size);
    /** Writes a contiguous subset of the local row to some of the remote nodes.
     */
    void put(vector<uint32_t> receiver_ranks, long long int offset,
             long long int size);
    /** Does a TCP sync with each member of the SST. */
    void sync_with_members() const;
    /** Marks a row as frozen, so it will no longer update, and its
     * corresponding
     * node will not receive writes. */
    void freeze(int index);
    /** Deletes all predicates currently registered with this SST. */
    void delete_all_predicates();

    class Predicates;
    /** Predicate management object for this SST. */
    Predicates &predicates;
    friend class Predicates;

    /**
     * Retrieve a previously-stored named predicate and call it.
     */
    template <NameEnum name>
    auto call_named_predicate(volatile const InternalRow &ir) const {
        constexpr int index = static_cast<int>(name);
        constexpr int max = std::tuple_size<named_functions_t>::value;
        static_assert(index < max,
                      "Error: this name was not used to name a RowPredicate");

        // the modulos here ensure the function type-checks
        // even when the index is too large.
        return std::get<index % max>(named_functions)(ir);
    }

    template <NameEnum name>
    auto call_named_predicate(const int row_index) const {
        return call_named_predicate<name>((*this)[row_index]);
    }
};

} /* namespace sst */

#include "sst_impl.h"

#endif /* SST_H */
