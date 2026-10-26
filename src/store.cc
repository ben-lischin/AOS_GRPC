#include "threadpool.h"

#include <cstdlib>
#include <iostream>
#include <atomic>
#include <chrono>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "store.grpc.pb.h"
#include "vendor.grpc.pb.h"



// max wait time for a vendor to respond
const auto VENDOR_DEADLINE = std::chrono::seconds(5);

/////////////
// Vendors //
/////////////

struct VendorInfo {
  std::string name;
  std::vector<std::unique_ptr<vendor::Vendor::Stub>> stubs;
  // round-robin counter - process-global but vendor-specific
  std::atomic<size_t> next_backend{0};
};

typedef std::vector<std::unique_ptr<VendorInfo>> VendorList;

// run on startup, build VendorList from vendor input file
bool load_vendors(const std::string &path, VendorList &vendors) {
  std::ifstream file(path);
  if (!file.is_open()) {
    std::cerr << "Failed to open vendor input file: " << path << std::endl;
    return false;
  }

  std::string line;
  while (std::getline(file, line)) {
    if (line.empty()) continue; // skip empty lines
    if (line[0] != '-') { // i.e. "Vendor A"
      vendors.push_back(std::make_unique<VendorInfo>());
      vendors.back()->name = line;
    } else { // i.e. "-localhost:50051", under a vendor
      std::string addr = line.substr(1); // remove "-"
      auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
      vendors.back()->stubs.push_back(vendor::Vendor::NewStub(channel));
    }
  }
  return !vendors.empty();
}

// for in-flight vendor requests
struct PendingBid {
  grpc::ClientContext context;
  vendor::BidReply reply;
  grpc::Status status;
  std::unique_ptr<grpc::ClientAsyncResponseReader<vendor::BidReply>> rpc; // handle for an in-flight call
};

// get bid from every vendor
store::ProductReply collect_bids(VendorList &vendors, const std::string &product) {
	vendor::BidQuery query;
	query.set_product_name(product);

	// private completion queue for this request
	grpc::CompletionQueue completion_queue;
	size_t num_vendors = vendors.size();
	std::vector<PendingBid> calls(num_vendors);
	auto deadline = std::chrono::system_clock::now() + VENDOR_DEADLINE;

	// start vendor calls
	// dont wait for response before calling next
	for (size_t i = 0; i < num_vendors; ++i) {
		VendorInfo &v = *vendors[i];

		// round-robin
		// fetch_add() atomically uses current counter and increments it for next use
		// "which backend for the vendor?"
		size_t backend_index = v.next_backend.fetch_add(1) % v.stubs.size();

		calls[i].context.set_deadline(deadline);
		calls[i].rpc = v.stubs[backend_index]->AsyncgetProductBid(&calls[i].context, query, &completion_queue);
		calls[i].rpc->Finish(&calls[i].reply, &calls[i].status, &calls[i]);
	}

	// wait for all call completions, in whatever order they arrive
	for (size_t i = 0; i < num_vendors; i++) { // we give the calls a dealine, so this should never hang
		void* tag;
		bool ok;
		completion_queue.Next(&tag, &ok);
	}

	// collect successful bids in vendor order
	store::ProductReply reply;
	for (size_t i = 0; i < num_vendors; i++) {
		if (calls[i].status.ok()) {
			store::ProductInfo *info = reply.add_products();
			info->set_price(calls[i].reply.price());
			info->set_vendor_id(calls[i].reply.vendor_id());
		}
		// failures skipped
	}

	// clean up - shutdown/drain completion queue
	completion_queue.Shutdown();
	void* tag;
	bool ok;
	while (completion_queue.Next(&tag, &ok)) {}
	return reply;
}



class ClientRequest {
	public:
		// Register with gRPC to receive the next getProducts request.
		ClientRequest(store::Store::AsyncService* service, grpc::ServerCompletionQueue* cq, threadpool* pool, VendorList* vendors)
			: _service(service), _completion_queue(cq), _pool(pool), _vendors(vendors), _responder(&_ctx), _state(WAITING_FOR_REQUEST) {
			_service->RequestgetProducts(&_ctx, &_request, &_responder, _completion_queue, _completion_queue, this);
		}

		// Main thread only: called when one of this request's operations completes.
		void on_event(bool ok) {
			// item taken off the completion queue
			if (_state == WAITING_FOR_REQUEST) {
				if (!ok) {
					delete this;
					return;
				}

				// register the next waiter first
				new ClientRequest(_service, _completion_queue, _pool, _vendors);

				// set the state before handing off
				_state = SENDING_REPLY;
				_pool->enqueue_job([this] { handle_request(); });
			} else {  // SENDING_REPLY
				// request finished processing, so it was added back to the completion queue, safe to clean
				delete this;
			}
		}

	private:
		// shared state, owned by main
		store::Store::AsyncService *_service;
		grpc::ServerCompletionQueue *_completion_queue;
		threadpool *_pool;
		VendorList *_vendors;

		// per request
		grpc::ServerContext _ctx;
		store::ProductQuery _request;
		grpc::ServerAsyncResponseWriter<store::ProductReply> _responder;

		enum State { WAITING_FOR_REQUEST, SENDING_REPLY };
		State _state;

		// worker thread to query vendors and send the reply: query vendors and send the reply
		void handle_request() {
			store::ProductReply reply = collect_bids(*_vendors, _request.product_name());
			// always OK, even if 0 bids
			_responder.Finish(reply, grpc::Status::OK, this);
		}
};


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
