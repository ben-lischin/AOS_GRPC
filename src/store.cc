#include "threadpool.h"
#include "vendors.h"
#include "client_req.h"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include <grpcpp/grpcpp.h>
#include "store.grpc.pb.h"


// the store itself, owns everything that lives for the program
class Store {
	public:
		Store(VendorList vendors, int max_threads): _vendors(std::move(vendors)), _pool(max_threads) {} // cant just copy vendors (unique_ptrs)

		// Start listening, then handle events forever. Returns false if startup fails.
		bool run(const std::string &listen_addr) {
			grpc::ServerBuilder builder;
			builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
			builder.RegisterService(&_service);
			_completion_queue = builder.AddCompletionQueue();
			_server = builder.BuildAndStart();
			if (!_server) {
				return false;
			}

			// create the first request slot
			new ClientRequest(&_service, _completion_queue.get(), &_pool, &_vendors);
			void* tag;
			bool ok;
			// receive, first waiting on that initial slot
			while (_completion_queue->Next(&tag, &ok)) {
				static_cast<ClientRequest*>(tag)->on_event(ok);
			}
			return true;
		}

	private:
		VendorList _vendors;
		store::Store::AsyncService _service;
		std::unique_ptr<grpc::ServerCompletionQueue> _completion_queue;
		std::unique_ptr<grpc::Server> _server;
		threadpool _pool; // one pool of worker threads for the whole store; one job per client request
};




int main(int argc, char** argv) {
	// ./store <vendor_file> <ip:port> <max_threads>
	if (argc != 4) {
		std::cerr << "Format: ./store <vendor_file> <ip:port> <max_threads>" << std::endl;
		return EXIT_FAILURE;
	}
	std::string vendor_file = argv[1];
	std::string listen_port = argv[2];
	int max_threads = std::atoi(argv[3]);
	if (max_threads < 1) { // need threads
		max_threads = 1;
	}

	VendorList vendors;
	if (!load_vendors(vendor_file, vendors)) {
		return EXIT_FAILURE;
	}

	Store store_server(std::move(vendors), max_threads);
	return store_server.run(listen_port) ? EXIT_SUCCESS : EXIT_FAILURE;
}
