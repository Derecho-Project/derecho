src=../verbs.cpp ../../connection_manager.cpp ../../rdmc/connection.cpp statistics.cpp timing.cpp
hdr=../verbs.h statistics.h timing.h
sst_hdr=../sst.h ../sst_impl.h ../predicates.h ../named_function.h ../args-finder.hpp ../combinators.h ../combinator_utils.h ../NamedRowPredicates.h ../util.h
options=-lrdmacm -libverbs -lrt -lpthread -O1 -g -Wall -Wno-unused-function -Wno-unused-variable -fno-omit-frame-pointer -Wno-unused-but-set-variable -Wno-unused-result
binaries=test test_write two_connections raw_rdma_read raw_rdma_write remote_read remote_write read_avg_time write_avg_time read_write_avg_time sequential_remote_read sequential_remote_write sequential_remote_read_write thread_sequential_remote_read parallel_post_poll random_thread_reads atomicity_test strcpy_atomicity_test integer_atomicity_test memcpy_atomicity_test simple_predicate count_read count_write predicates_per_second predicate_row_scaling_read predicate_row_scaling_write row_size_scaling_write row_size_scaling_read average_load_pred token_passing named_predicate_test test_failure_handling multicast_throughput multicast_latency time_skew_experiment

all : $(binaries)

test : test.cpp $(src) $(hdr)
	c++ -std=c++14 test.cpp $(src) -o test $(options)

test_write : test_write.cpp $(src) $(hdr)
	c++ -std=c++14 test_write.cpp $(src) -o test_write $(options)

test_num_polled : test_num_polled.cpp $(src) $(hdr)
	c++ -std=c++14 test_num_polled.cpp $(src) -o test_num_polled $(options)

two_connections : two_connections.cpp $(src) $(hdr)
	c++ -std=c++14 two_connections.cpp $(src) -o two_connections $(options)

raw_rdma_read : raw_rdma_read.cpp $(src) $(hdr)
	c++ -std=c++14 raw_rdma_read.cpp $(src) -o raw_rdma_read $(options)

raw_rdma_write : raw_rdma_write.cpp $(src) $(hdr)
	c++ -std=c++14 raw_rdma_write.cpp $(src) -o raw_rdma_write $(options)

remote_read : remote_read.cpp $(src) $(hdr)
	c++ -std=c++14 remote_read.cpp $(src) -o remote_read $(options)

remote_write : remote_write.cpp $(src) $(hdr)
	c++ -std=c++14 remote_write.cpp $(src) -o remote_write $(options)

read_avg_time : read_avg_time.cpp $(src) $(hdr)
	c++ -std=c++14 read_avg_time.cpp $(src) -o read_avg_time $(options)

write_avg_time : write_avg_time.cpp $(src) $(hdr)
	c++ -std=c++14 write_avg_time.cpp $(src) -o write_avg_time $(options)

read_write_avg_time : read_write_avg_time.cpp $(src) $(hdr)
	c++ -std=c++14 read_write_avg_time.cpp $(src) -o read_write_avg_time $(options)

sequential_remote_read : sequential_remote_read.cpp $(src) $(hdr)
	c++ -std=c++14 sequential_remote_read.cpp $(src) -o sequential_remote_read $(options)

sequential_remote_write : sequential_remote_write.cpp $(src) $(hdr)
	c++ -std=c++14 sequential_remote_write.cpp $(src) -o sequential_remote_write $(options)

sequential_remote_read_write : sequential_remote_read_write.cpp $(src) $(hdr)
	c++ -std=c++14 sequential_remote_read_write.cpp $(src) -o sequential_remote_read_write $(options)

thread_sequential_remote_read : thread_sequential_remote_read.cpp $(src) $(hdr)
	c++ -std=c++14 thread_sequential_remote_read.cpp $(src) -o thread_sequential_remote_read $(options)

parallel_post_poll : parallel_post_poll.cpp $(src) $(hdr)
	c++ -std=c++14 parallel_post_poll.cpp $(src) -o parallel_post_poll $(options)

random_thread_reads : random_thread_reads.cpp $(src) $(hdr)
	c++ -std=c++14 random_thread_reads.cpp $(src) -o random_thread_reads $(options)

atomicity_test : atomicity_test.cpp $(src) $(hdr)
	c++ -std=c++14 atomicity_test.cpp $(src) -o atomicity_test $(options)

strcpy_atomicity_test : strcpy_atomicity_test.cpp $(src) $(hdr)
	c++ -std=c++14 strcpy_atomicity_test.cpp $(src) -o strcpy_atomicity_test $(options)

integer_atomicity_test : integer_atomicity_test.cpp $(src) $(hdr)
	c++ -std=c++14 integer_atomicity_test.cpp $(src) -o integer_atomicity_test $(options)

memcpy_atomicity_test : memcpy_atomicity_test.cpp $(src) $(hdr)
	c++ -std=c++14 memcpy_atomicity_test.cpp $(src) -o memcpy_atomicity_test $(options)

simple_predicate : simple_predicate.cpp $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 simple_predicate.cpp $(src) -o simple_predicate $(options)

count_read : count_read.cpp $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 count_read.cpp $(src) -o count_read $(options)

count_write : count_write.cpp $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 count_write.cpp $(src) -o count_write $(options)

predicate_row_scaling_read : predicate_row_scaling_read.cpp $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 predicate_row_scaling_read.cpp $(src) -o predicate_row_scaling_read $(options)

predicate_row_scaling_write : predicate_row_scaling_write.cpp $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 predicate_row_scaling_write.cpp $(src) -o predicate_row_scaling_write $(options)

predicates_per_second : predicates_per_second.cpp $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 predicates_per_second.cpp $(src) -o predicates_per_second $(options)

row_size_scaling_read: row_size_scaling_read.cpp row_size_scaling.h $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 row_size_scaling_read.cpp $(src) -o row_size_scaling_read $(options)

row_size_scaling_write: row_size_scaling_write.cpp row_size_scaling.h $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 row_size_scaling_write.cpp $(src) -o row_size_scaling_write $(options)

average_load_pred : average_load_pred.cpp $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 average_load_pred.cpp $(src) -o average_load_pred $(options)

token_passing : token_passing.cpp $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 token_passing.cpp $(src) -o token_passing $(options)

named_predicate_test : named_predicate_test.cpp $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 named_predicate_test.cpp $(src) -o named_predicate_test $(options)

test_failure_handling : test_failure_handling.cpp $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 test_failure_handling.cpp $(src) -o test_failure_handling $(options)

multicast_throughput : multicast_throughput.cpp multicast.h ../../max_members.h $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 multicast_throughput.cpp ../../experiments/aggregate_bandwidth.cpp $(src) -o multicast_throughput $(options)

multicast_latency : multicast_latency.cpp multicast.h ../../max_members.h $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 multicast_latency.cpp $(src) -o multicast_latency $(options)

time_skew_experiment : time_skew_experiment.cpp time_skew.h $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 time_skew_experiment.cpp $(src) -o time_skew_experiment $(options)

selective_put_test : selective_put_test.cpp $(src) $(hdr) $(sst_hdr)
	c++ -std=c++14 selective_put_test.cpp $(src) -o selective_put_test $(options)

clean :
	rm -f $(binaries) *~
