#include "store/commit_log.hh"
#include "store/checked_file_impl.hh"
#include "store/util/coding.hh"
#include "store/util/crc32c.hh"
#include "store/log_format.hh"
#include "store/priority_manager.hh"
#include "core/align.hh"
namespace store {

static void init_type_crc(uint32_t* type_crc) {
  for (int i = 0; i <= MAX_RECORD_TYPE; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::value(&t, 1);
  }
}

commit_log::commit_log(sstring fn)
    : _file_name (std::move(fn))
    , _writer(nullptr)
    , _current_buffer()
    , segement_offset_(0)
    , close_(false)
    , _released_semaphore(1)
    , _pending_semaphore(MAX_FLUSH_BUFFER_SIZE)
{
    init_type_crc(type_crc_);
}

future<flush_buffer> commit_log::do_flush_one_buffer(flush_buffer fb)
{
    // flush buffer to file
    assert(_writer);
    return _writer->write(fb).then([fb] {
        return make_ready_future<flush_buffer>(fb);
    });
}

future<> commit_log::initialize()
{
    repeat([this] {
    return _pending_semaphore.wait().then([this] {
        auto b = _pending_buffers.front();
        _pending_buffers.pop();
        return do_flush_one_buffer(b).then([this] (auto b) {
            b.reset();
            _released_buffers.push(b);
            _released_semaphore.signal();
            auto should_stop = _pending_buffers.empty() && close_;
                return make_ready_future<stop_iteration>(should_stop ? stop_iteration::yes : stop_iteration::no);
            });
        });
    });

    file_open_options opt;
    opt.extent_allocation_size_hint = IO_EXTENT_ALLOCATION_SIZE;
    return open_checked_file_dma(commit_error_handler, _file_name, open_flags::wo | open_flags::create, opt).then([this](file f) {
        // xfs doesn't like files extended betond eof, so enlarge the file
        return f.truncate(IO_EXTENT_ALLOCATION_SIZE).then([this, f] () mutable {
            _writer = make_lw_shared<store::log_writer>(std::move(f));
            return make_ready_future<>();
        });
    });
}
flush_buffer commit_log::make_flush_buffer()
{
     auto b = ::memalign(OUTPUT_BUFFER_ALIGNMENT, FLUSH_BUFFER_SIZE);
     if (!b) {
         throw std::bad_alloc();
     }
     return flush_buffer(reinterpret_cast<char *>(b), FLUSH_BUFFER_SIZE);
}
future<> commit_log::make_room_for_apending_mutation(size_t size)
{
    if (!_current_buffer.available() || _current_buffer.available_size() < size) {
        _current_buffer.close();
        _pending_buffers.push(_current_buffer);
        _pending_semaphore.signal();
        return _released_semaphore.wait().then([this] {
            if (!_released_buffers.empty()) {
                _current_buffer = _released_buffers.front();
                _released_buffers.pop();
            }
            _current_buffer = make_flush_buffer();
            return make_ready_future<>();
        });
    }
    return make_ready_future<>();
}

future<> commit_log::append(lw_shared_ptr<mutation> m)
{
    if (!_writer) {
        return initialize().then([this, m = std::move(m)] {
            return this->append(m);
        });
    }
    assert(_writer);
    auto estimated_size = m->estimate_serialized_size() + HEADER_SIZE;
    return make_room_for_apending_mutation(estimated_size).then([this, m, estimated_size] {
        // Format the header
        char* header = _current_buffer.get_current();
        _current_buffer.skip(HEADER_SIZE);
        auto record = _current_buffer.get_current();
        auto serialized_size = _current_buffer.write(m);
        uint32_t crc = crc32c::extend(type_crc_[record_type::full], record, serialized_size);
        crc = crc32c::mask(crc);
        // [0-3] 4bytes for crc
        encode_fixed32(header, crc);
        header[4] = static_cast<char>(serialized_size & 0xff);
        header[5] = static_cast<char>(serialized_size >> 8);
        header[6] = static_cast<char>(record_type::full);
    });
}

lw_shared_ptr<commit_log> make_commit_log(sstring file_name)
{
    return make_lw_shared<commit_log>(std::move(file_name));
}
}
