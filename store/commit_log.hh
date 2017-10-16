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
    commit_log() = default;
    commit_log(bytes fn) : _file_name (std::move(fn)), _writer(make_lw_shared<log::writer>(_file_name)) {}
    ~commit_log() = default;
    commit_log(commit_log&&) = default;
    commit_log(const commit_log&) = delete;
    commit_log& operator = (const commit_log&) = delete;
    commit_log& operator = (commit_log&&) = default;
    future<> append(partition&& entry);
};
}
