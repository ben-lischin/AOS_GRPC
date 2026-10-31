Overview
- This project builds a `Store` server that clients ask for the price of a product. The store has no prices itself, so for every query it asks each registered vendor for a bid and returns all the bids to the client
- Each vendor runs several identical backend servers, only one of which is queried by a single client request. We distribute load across these in a round-robin fashion (atomic increment, mod vendor count), so many client requests aren't forwarded to the same vendor backend each time
- We use asynchronous GRPC communication via completion queues. The store is an async server to clients and an async client to vendors, so it never sits blocked on a single call and all vendor calls for a query run at the same time
- Requests are processed on the store's own thread pool: the main thread only accepts requests and hands them off, and a pool worker contacts the vendors, collects the bids, and replies
- If a vendor request fails or times out for any reason, or if the relevant backend is down, that vendor is skipped by the store and the client still gets the bids from all the other vendors that responded

Threadpool
- We have a single fixed-size threadpool for the whole store's lifetime, to handle every client request. Every incoming client request will go into its one job queue, then any free worker can take any request
- There is a shared job queue synchronized across requests and workers by a mutex lock. New requests get added to the queue, and when a worker frees up in the pool to begin new work, it will grab from the head of this job queue.
  - Idle workers block on a condition variable until there is a new job added (or we signal a shutdown)
  - The job itself from the queue is executed asynchronously outside the mutex, so the lock's critical section is just the queue's push/pop
- Shutdown (from the destructor) set the stop signal, notify all workers, and join all threads. Workers finish their current jobs if any, drain the queue, drain the remaining fixed queue of jobs before exiting (stop signal blocks new enqueues from requests)

Client side: `Store` as async server for client requests
- There is a server completion queue for the whole store, which the main thread loops over (blocks until an event enters the queue), and calls `on_event()` on the `ClientRequest` associated with the tag
- Each `ClientRequest` starts two operations with `this` as the tag; each posts one event to the server CQ when it finishes:
  - A request starts in state `WAITING_FOR_REQUEST`, and `RequestgetProducts()` in the constructor registers it with GRPC
  - `Next()` picks it up in the completion queue, and to handle this event we: create the next `ClientRequest` first (so new clients can always connect), set this request's state to `SENDING_REPLY`, then enqueue a job to handle this request (collects bids and calls `Finish()` from the worker on its last use of the object)
  - The request re-enters the completion queue, for a second pass where now we can finally delete `this` request. This has to be a separate event in the completion queue so we don't risk freeing memory that GRPC may still be using

Vendor side: `Store` as async client
- My code here assumes a well-formed vendor file following the expected format and content guide, and may not behave well otherwise
- Each request uses its own vendor completion queue, created and destroyed within a `collect_bids()` call, so workers never share a queue or see each other's events:
  - Start one call per vendor (round-robin between its different backends), with a deadline to ensure nothing hangs. Nothing blocks, so the vendor calls can execute in their respective backends in parallel, hiding most of the vendor bids latency behind approximately the slowest vendor call
  - `Next()` on the completion queue to process each call as they come back
  - Add successful bids to the reply, any failures/timeouts are skipped
- Per-call state (`PendingBids`) is in a vector sized once, so it has static locations for GRPC to write into it

References
- gRPC C++ async helloworld examples (greeter_async_server.cc, greeter_async_client.cc): https://github.com/grpc/grpc/tree/master/examples/cpp/helloworld
- gRPC C++ API docs: https://grpc.github.io/grpc/cpp/