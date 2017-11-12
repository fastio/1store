#pragma once
#include "store/log_writer.hh"
#include "partition.hh"
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "utils/bytes.hh"
namespace store {
class commit_log {
    bytes _file_name;
    lw_shared_ptr<log::writer> _writer;
public:
    commit_log();
    explicit commit_log(bytes fn);
    ~commit_log();
    commit_log(const commit_log&) = delete;
    commit_log& operator = (const commit_log&) = delete;
    future<> append(partition&& entry);
};
}
