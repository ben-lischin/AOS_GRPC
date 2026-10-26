#pragma once

#include <condition_variable>
#include <cstddef>
#include <functinoal>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class threadpool {
	public:
        threadpool(size_t num_threads) {
            if (num_threads < 1) { // non-empty thread pool
                num_threads = 1;
            }

            for (size_t i = 0; i < num_threads; i++) {
                _workers.emplace_back([this] { _worker_func(); }); // lambda syntax
            }
        }

        ~threadpool() { // teardown
            {
                std::scoped_lock lock(_queue_mutex);
                _stop_signal = true;
            }
            _worker_cond.notify_all();
            for (std::thread& t : _workers) {
                t.join();
            }
        }

        void enqueue_job(std::function<void()> job) { // add job to queue
            {
                std::scoped_lock lock(_queue_mutex);
                if (_stop_signal) { // block new enqueues
                    return;
                }
                _jobs.push(job);
            }
            _worker_cond.notify_one(); // one workerto handle
        }

    private:
        std::vector<std::thread> _workers; // workers to execute jobs
        std::queue<std::function<void()>> _jobs; // jobs to execute
        std::mutex _queue_mutex; // jobs queue, synchrnoization
        std::condition_variable _worker_cond; // worker sleeps until there's work
        bool _stop_signal = false; // teardown stop signal. in-flight jobs finish, but then remaining jobs in the queue won't get picked up. also blocks new job requests

        void _worker_func() {
            while (1) {
                std::function<void()> job;
                {
                    std::unique_lock<std::mutex> lock(_queue_mutex);
                    _worker_cond.wait(lock, [this] { return _stop_signal || !_jobs.empty(); });
                    if (_stop_signal && _jobs.empty()) {
                        return;
                    }
                    job = _jobs.front();
                    _jobs.pop();
                }
                job();
            }
        }

};

