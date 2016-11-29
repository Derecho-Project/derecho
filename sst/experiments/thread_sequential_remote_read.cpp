#include <iostream>
#include <map>
#include <vector>
#include <fstream>
#include <cstdlib>
#include <pthread.h>
#include <string.h>

#include "sst/tcp.h"
#include "sst/verbs.h"

using std::string;
using std::vector;
using std::map;
using std::cout;
using std::endl;
using std::cin;
using std::ofstream;

using namespace sst;
using namespace sst::tcp;

// threads set this to indicate they have established connection and are ready to read
vector <int> ready_to_read;
// main sets this to set off the threads
int send = 0;
// threads set this to indicate finished read operations
vector <int> sent_successfully;

// size of data transfers
int size = 10;

// maximum size
int max_size = 10000;

// step size
int step_size = 100;

// number of remote reads posted
int num_reruns = 1000;

void *run (void *tid) {
  int thread_id = (long) tid;
  cout << "Thread number " << thread_id << " starting" << endl;
  // no need for thread 0
  if (thread_id == 0) {
    ready_to_read[0] = 1;
    sent_successfully[0] = 1;
    pthread_exit (NULL);
  }

  while (size != max_size) {
    // create buffer for write and read
    char *write_buf, *read_buf;
    write_buf = (char *) malloc (size);
    read_buf = (char *) malloc (size);

    // create connections
    resources *res = new resources (thread_id, write_buf, read_buf, size, size);

    cout << "Connection established to remote node " << thread_id << endl;

    // set ready to read
    ready_to_read [thread_id] = 1;
  
    while (send == 0) {
    
    }

    for (int i = 0; i < num_reruns; ++i) {
      res->post_remote_read (size);
      verbs_poll_completion();
    }

    // set sent successfully to indicate it to the main thread
    sent_successfully [thread_id] = 1;

    // sync to notify the remote node
    char  temp_char; 
    char tQ[2] = {'Q', 0};
    // wait for node 0 to finish read
    sock_sync_data(get_socket (thread_id), 1, tQ, &temp_char);
    // free malloc()ed area
    free (write_buf);
    free (read_buf);
    delete (res);
  }

  cout << "Thread " << thread_id << " exiting" << endl;
  pthread_exit (NULL);
}


void initialize(int node_rank, const map <uint32_t, string> & ip_addrs) {
  // initialize tcp connections
  tcp_initialize(node_rank, ip_addrs);
  
  // initialize the rdma resources
  verbs_initialize();
}

int main () {
  ofstream fout;
  fout.open("data_thread_sequential_remote_read.csv");
  
  // input number of nodes and the local node id
  int num_nodes, node_rank;
  cin >> num_nodes;
  cin >> node_rank;

  // input the ip addresses
  map <uint32_t, string> ip_addrs;
  for (int i = 0; i < num_nodes; ++i) {
    cin >> ip_addrs[i];
  }

  // create all tcp connections and initialize global rdma resources
  initialize(node_rank, ip_addrs);

  ready_to_read.resize (num_nodes, 0);
  sent_successfully.resize (num_nodes, 0);  
  
  // only node rank 0 reads
  if (node_rank == 0) {
    pthread_t threads[num_nodes];
    pthread_attr_t attr;
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    cout << "Creating threads" << endl;
    for (long i = 1; i < num_nodes; ++i) {
      int rc = pthread_create(&threads[i], NULL, run, (void *) i);
      if (rc) {
	cout << "Error in thread creation" << endl;
	exit (-1);
      }
    }

    // check to see that all threads have established connection
    bool ready = false;
    while (ready == false) {
      ready = true;
      for (int i = 1; i < num_nodes; ++i) {
	if (ready_to_read[i] == 0) {
	  ready = false;
	}
      }
    }

    for (size = 10; size < max_size;) {
      // start the timing experiment
      struct timespec start_time;
      struct timespec end_time;
      long long int nanoseconds_elapsed;

      clock_gettime(CLOCK_REALTIME, &start_time);
    
      // set off the threads
      send = 1;

      // check to see that all threads have finished reading
      ready = false;
      while (ready == false) {
	ready = true;
	for (int i = 1; i < num_nodes; ++i) {
	  if (sent_successfully[i] == 0) {
	    ready = false;
	  }
	}
      }
    
      clock_gettime(CLOCK_REALTIME, &end_time);
      nanoseconds_elapsed = (end_time.tv_sec-start_time.tv_sec)*1000000000 + (end_time.tv_nsec-start_time.tv_nsec);
    
      fout << size << ", " << (nanoseconds_elapsed+0.0)/(1000*num_reruns*(num_nodes-1)) << endl;

      send = 0;
      for (int i = 1; i < num_nodes; ++i) {
	sent_successfully[i] = 0;
      }
      
      if (size <2000) {
	size++;
      }
      else {
	size+=step_size;
      }
    }
    // wait for threads to exit
    void *status;
    // pthread_attr_destroy (&attr);
    for (int i = 0; i < num_nodes; ++i) {
      int rc = pthread_join(threads[i], &status);
      if (rc) {
	cout << "Error in thread join " << endl;
      }
    }
    cout << "All threads terminated. Exiting!!" << endl;
  }

  else {
    for (size = 10; size < max_size;) {
      // create a resource with node 0
      // create buffer for write and read
      char *write_buf, *read_buf;
      write_buf = (char *) malloc (size);
      read_buf = (char *) malloc (size);

      // write to the write buffer
      for (int i = 0; i < size-1; ++i) {
	write_buf[i] = 'a'+node_rank;
      }
      write_buf[size-1] = 0;

      resources *res = new resources (0, write_buf, read_buf, size, size);
      char  temp_char; 
      char tQ[2] = {'Q', 0};
      // wait for node 0 to finish read
      sock_sync_data(get_socket (0), 1, tQ, &temp_char);

      if (size <2000) {
	size++;
      }
      else {
	size+=step_size;
      }
      // free malloc()ed area
      free (write_buf);
      free (read_buf);
      // destroy the resource
      delete(res);
    }
  }

  verbs_destroy ();
  fout.close();
  return 0;
}
