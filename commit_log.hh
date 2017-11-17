#pragma once
#include "mutation.hh"
#include "core/future.hh"
#include "core/queue.hh"
#include "core/semaphore.hh"
#include "core/shared_ptr.hh"
#include "core/sstring.hh"
#include <queue>
namespace store {

class flush_buffer;

class commit_log {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    explicit commit_log(std::unique_ptr<impl>);
    ~commit_log();
    commit_log(const commit_log&) = delete;
    commit_log& operator = (const commit_log&) = delete;
    future<> append(lw_shared_ptr<mutation> entry);
    future<> close();
    friend lw_shared_ptr<commit_log> make_commit_log();
};

lw_shared_ptr<commit_log> make_commit_log();
}
