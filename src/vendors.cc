#include "vendors.h"

#include <chrono>
#include <fstream>
#include <iostream>

// max wait time for a vendor to respond
const auto VENDOR_DEADLINE = std::chrono::seconds(5);

// for in-flight vendor requests
struct PendingBid {
  grpc::ClientContext context;
  vendor::BidReply reply;
  grpc::Status status;
  std::unique_ptr<grpc::ClientAsyncResponseReader<vendor::BidReply>> rpc; // handle for an in-flight call
};

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
		size_t backend_index = v.next_backend.fetch_add(1) % v.stubs.size();

		calls[i].context.set_deadline(deadline);
		calls[i].rpc = v.stubs[backend_index]->AsyncgetProductBid(&calls[i].context, query, &completion_queue);
		calls[i].rpc->Finish(&calls[i].reply, &calls[i].status, &calls[i]);
	}

	// wait for all call completions, in whatever order they arrive
	for (size_t i = 0; i < num_vendors; i++) { // we give the calls a dealine, so this should never hang
		void *tag;
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
	void *tag;
	bool ok;
	while (completion_queue.Next(&tag, &ok)) {}
	return reply;
}