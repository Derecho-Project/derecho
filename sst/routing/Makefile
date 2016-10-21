src=dijkstra.cpp routing.cpp ../verbs.cpp ../tcp.cpp ../experiments/statistics.cpp ../experiments/timing.cpp
hdr=lsdb_row.h dijkstra.h routing.h std_hashes.h ../verbs.h ../tcp.h ../sst.h ../predicates.h ../named_function.h ../util.h ../args-finder.hpp ../experiments/statistics.h ../experiments/timing.h
options=-lrdmacm -libverbs -lrt -lpthread -O1 -g -Wall -Wno-unused-function -Wno-unused-variable -fno-omit-frame-pointer -Wno-unused-but-set-variable -Wno-unused-result
binaries=router_experiment

all : $(binaries)

router_experiment: router_experiment.cpp $(src) $(hdr)
	g++ -std=c++14 router_experiment.cpp $(src) -o router_experiment $(options)

local_compute_timing: local_compute_timing.cpp $(src) $(hdr)
	g++ -std=c++14 local_compute_timing.cpp $(src) -o local_compute_timing $(options)

dijkstra_test : dijkstra.cpp dijkstra.h
	g++ -std=c++14 dijkstra.cpp -o dijkstra_test $(options)

clean :
	rm -f $(binaries)
