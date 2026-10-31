#pragma once


#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "store.grpc.pb.h"
#include "vendor.grpc.pb.h"


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
bool load_vendors(const std::string &path, VendorList &vendors);

// get bid from every vendor
store::ProductReply collect_bids(VendorList &vendors, const std::string &product);
