#pragma once
#include "store/log_writer.hh"
#include "partition.hh"
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "core/sstring.hh"
namespace store {
class commit_log {
    sstring _file_name;
    lw_shared_ptr<log::writer> _writer;
public:
    commit_log();
    explicit commit_log(sstring fn);
    ~commit_log() {}
    commit_log(const commit_log&) = delete;
    commit_log& operator = (const commit_log&) = delete;
    future<> append(lw_shared_ptr<partition> entry);
private:
    future<> initialize();
};

lw_shared_ptr<commit_log> make_commit_log(sstring file_name);
}
