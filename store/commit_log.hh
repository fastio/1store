#pragma once
#include "store/log_writer.hh"
#include "mutation.hh"
#include "core/future.hh"
#include "core/queue.hh"
#include "core/semaphore.hh"
#include "core/shared_ptr.hh"
#include "core/sstring.hh"
#include <queue>
namespace store {
class commit_log {
    sstring _file_name;
    lw_shared_ptr<log_writer> _writer;
    static const size_t IO_EXTENT_ALLOCATION_SIZE = 32 * 1024 * 1024;
    static const size_t FLUSH_BUFFER_SIZE = 1024 * 1024 * 1024;
    static const size_t SEGEMENT_SIZE = 32 * 1024;
    static const size_t OUTPUT_BUFFER_ALIGNMENT = 4096;
    static const uint32_t MAX_FLUSH_BUFFER_SIZE = 32;
    std::queue<flush_buffer> _released_buffers;
    std::queue<flush_buffer> _pending_buffers;
    flush_buffer _current_buffer;
    size_t segement_offset_;
    bool close_ { false };
    using timeout_exception_factory = default_timeout_exception_factory;
    basic_semaphore<timeout_exception_factory> _released_semaphore;
    basic_semaphore<timeout_exception_factory> _pending_semaphore;
    uint32_t type_crc_[MAX_RECORD_TYPE + 1];
public:
    explicit commit_log(sstring fn);
    ~commit_log() {}
    commit_log(const commit_log&) = delete;
    commit_log& operator = (const commit_log&) = delete;
    future<> append(lw_shared_ptr<mutation> entry);
private:
    future<> initialize();
    future<> make_room_for_apending_mutation(size_t size);
    future<flush_buffer> do_flush_one_buffer(flush_buffer fb);
    flush_buffer make_flush_buffer();
};

lw_shared_ptr<commit_log> make_commit_log(sstring file_name);
}
