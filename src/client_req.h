#pragma once
#include <grpcpp/grpcpp.h>

#include "store.grpc.pb.h"
#include "threadpool.h"
#include "vendors.h"

class ClientRequest {
	public:
		// register with gRPC to receive the next getProducts request.
		ClientRequest(store::Store::AsyncService *service, grpc::ServerCompletionQueue *cq, threadpool *pool, VendorList *vendors)
			: _service(service), _completion_queue(cq), _pool(pool), _vendors(vendors), _responder(&_ctx), _state(WAITING_FOR_REQUEST) {
			_service->RequestgetProducts(&_ctx, &_request, &_responder, _completion_queue, _completion_queue, this);
		}

		// main thread only: called when one of this request's operations completes.
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
			} else { // SENDING_REPLY
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
		ServerContext _ctx;
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
