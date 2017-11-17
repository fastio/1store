#include "commit_log.hh"
#include "store/checked_file_impl.hh"
#include "store/util/coding.hh"
#include "store/util/crc32c.hh"
#include "store/log_format.hh"
#include "store/priority_manager.hh"
#include "core/align.hh"
#include "core/gate.hh"
namespace store {

class flush_buffer final {
    lw_shared_ptr<temporary_buffer<char>> _data;
    data_output _output;
    size_t _offset;
    size_t _written;
    uint32_t _touched_counter;
    uint32_t _ref;
    gate _gate;
public:
    flush_buffer()
        : _data (nullptr)
        , _output(nullptr, size_t(0))
        , _offset(0)
        , _written(0)
        , _touched_counter(0)
    {
    }

    flush_buffer(char* data, size_t size)
        : _data(make_lw_shared<temporary_buffer<char>>(data, size, make_free_deleter(data)))
        , _output(data, size)
        , _offset(0)
        , _written(0)
        , _touched_counter(0)
    {
    }

    inline void enter() { _gate.enter(); }
    inline void leave() { _gate.leave(); }
    inline future<> close() { return _gate.close(); }

    inline void skip(size_t n) {
        _offset += n;
    }

    inline size_t write(lw_shared_ptr<mutation> m) {
        size_t serialized = m->encode_to(_output);
        _offset += serialized;
        return serialized;
    }

    inline char* get_current() {
        return _data->get_write() + _offset;
    }

    inline size_t available_size() const {
        return _data->size() - _offset;
    }

    inline bool available() const {
        return !!_data;
    }

    inline size_t payload_size() const {
        return _offset;
    }

    inline bool flushed_all() const {
        return _written >= _offset;
    }

    inline void update_flushed_size(size_t flushed) {
        _written += flushed;
    }

    inline char* data() {
        return _data->get_write() + _written;
    }

    inline size_t size() const {
        return _offset - _written;
    }

    inline void reset() {
        _offset = 0;
        _written = 0;
        _touched_counter = 0;
        _gate = {};
    }

    inline uint32_t touch() {
        ++_touched_counter;
        return _touched_counter;
    }
};

static void init_type_crc(uint32_t* type_crc) {
  for (int i = 0; i <= MAX_RECORD_TYPE; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::value(&t, 1);
  }
}

class commit_log::impl final {
    sstring _file_name;
    lw_shared_ptr<file> _file;
    static const size_t IO_EXTENT_ALLOCATION_SIZE = 32 * 1024 * 1024;
    static const size_t FLUSH_BUFFER_SIZE = 1024 * 1024 * 1024;
    static const size_t SEGEMENT_SIZE = 32 * 1024;
    static const size_t OUTPUT_BUFFER_ALIGNMENT = 4096;
    static const uint32_t MAX_FLUSH_BUFFER_SIZE = 32;
    static const size_t FLUSH_SIZE_THRESHOLD = FLUSH_BUFFER_SIZE * 80 / 100;
    static const uint32_t FLUSH_TOUCH_COUNTER_THRESHOLD = 10;
    std::queue<lw_shared_ptr<flush_buffer>> _released_buffers;
    std::queue<lw_shared_ptr<flush_buffer>> _pending_buffers;
    lw_shared_ptr<flush_buffer> _current_buffer;
    size_t _file_offset;
    bool _shutdown { false };

    using clock_type = lowres_clock;
    // periodically flush commitlog buffer to disk
    timer<clock_type> _timer;
    gate _gate;
    using timeout_exception_factory = default_timeout_exception_factory;
    basic_semaphore<timeout_exception_factory> _released_semaphore;
    basic_semaphore<timeout_exception_factory> _pending_semaphore;
    uint32_t type_crc_[MAX_RECORD_TYPE + 1];
public:
    explicit impl(sstring fn);
    ~impl() {}
    future<> append(lw_shared_ptr<mutation> entry);
    future<> close();
private:
    future<> initialize();
    future<> make_room_for_apending_mutation(size_t size);
    future<> do_flush_one_buffer(lw_shared_ptr<flush_buffer> fb);
    lw_shared_ptr<flush_buffer> make_flush_buffer();
    void on_timer();
};

commit_log::impl::impl(sstring fn)
    : _file_name (std::move(fn))
    , _file(nullptr)
    , _current_buffer(nullptr)
    , _file_offset(0)
    , _shutdown(false)
    , _released_semaphore(MAX_FLUSH_BUFFER_SIZE)
    , _pending_semaphore(0)
{
    init_type_crc(type_crc_);
}

future<> commit_log::impl::do_flush_one_buffer(lw_shared_ptr<flush_buffer> fb)
{
    // flush buffer to file
    assert(_file);
    return repeat([this, fb] () mutable {
        auto data = fb->data();
        auto size = align_up<size_t>(fb->size(), OUTPUT_BUFFER_ALIGNMENT);
        std::fill(data + fb->size(), data + size, 0);
        auto&& priority_class = get_local_commitlog_priority();
        return _file->dma_write(_file_offset, data, size, priority_class).then([this, fb] (auto s) {
            fb->update_flushed_size(s);
            s = align_down<size_t>(s, OUTPUT_BUFFER_ALIGNMENT);
            _file_offset += s;

            if (fb->flushed_all()) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    }).then ([this] {
         return _file->flush().then([] {
             return make_ready_future<>();
         });
    });
}

void commit_log::impl::on_timer()
{
    if (_current_buffer) {
        _current_buffer->touch();
        if ( _current_buffer->payload_size() > FLUSH_SIZE_THRESHOLD || _current_buffer->touch() > FLUSH_TOUCH_COUNTER_THRESHOLD) {
            _current_buffer->close().then([this] {
                _pending_buffers.emplace(_current_buffer);
                _pending_semaphore.signal();
                _current_buffer = nullptr; 
            });
        }
    }
    if (!_shutdown) {
        _timer.arm(std::chrono::milliseconds(8000));
    }
}

future<> commit_log::impl::initialize()
{
    repeat([this] {
    return _pending_semaphore.wait().then([this] {
        auto b = _pending_buffers.front();
        _pending_buffers.pop();
        return do_flush_one_buffer(b).then([this, released_buffer = b]  {
            released_buffer->reset();
            _released_buffers.emplace(released_buffer);
            _released_semaphore.signal();
            auto should_stop = _pending_buffers.empty() && _shutdown;
                return make_ready_future<stop_iteration>(should_stop ? stop_iteration::yes : stop_iteration::no);
            });
        });
    });

     _timer.set_callback(std::bind(&commit_log::impl::on_timer, this));
     _timer.arm(std::chrono::milliseconds(8000));

    file_open_options opt;
    opt.extent_allocation_size_hint = IO_EXTENT_ALLOCATION_SIZE;
    return open_checked_file_dma(commit_error_handler, _file_name, open_flags::wo | open_flags::create, opt).then([this](file f) {
        // xfs doesn't like files extended betond eof, so enlarge the file
        _file = make_lw_shared<file>(std::move(f));
        return _released_semaphore.wait().then([this] {
            _current_buffer = this->make_flush_buffer();
            return make_ready_future<>();
        });
    });
}
lw_shared_ptr<flush_buffer> commit_log::impl::make_flush_buffer()
{
     auto b = ::memalign(OUTPUT_BUFFER_ALIGNMENT, FLUSH_BUFFER_SIZE);
    // OUTPUT_BUFFER_ALIGNMENT, FLUSH_BUFFER_SIZE);
     if (!b) {
         throw std::bad_alloc();
     }
     auto s = FLUSH_BUFFER_SIZE;
     return make_lw_shared<flush_buffer>(reinterpret_cast<char *>(b), s);
}

future<> commit_log::impl::make_room_for_apending_mutation(size_t size)
{
    if (!_current_buffer || _current_buffer->available_size() < size) {
        if (_current_buffer) {
            _current_buffer->close();
            _pending_buffers.emplace(_current_buffer);
            _pending_semaphore.signal();
        }
        return _released_semaphore.wait().then([this] {
            if (!_released_buffers.empty()) {
                _current_buffer = _released_buffers.front();
                _released_buffers.pop();
            }
            else {
                _current_buffer = make_flush_buffer();
            }
            return make_ready_future<>();
        });
    }
    return make_ready_future<>();
}

future<> commit_log::impl::append(lw_shared_ptr<mutation> m)
{
    if (!_file) {
        return initialize().then([this, m = std::move(m)] {
            return this->append(m);
        });
    }
    _gate.enter();
    auto estimated_size = m->estimate_serialized_size() + HEADER_SIZE;
    return make_room_for_apending_mutation(estimated_size).then([this, m, estimated_size] {
        _current_buffer->enter();
        // Format the header
        char* header = _current_buffer->get_current();
        // Fill the header later, because we need to compute the CRC32 of mutation.
        _current_buffer->skip(HEADER_SIZE);
        auto record = _current_buffer->get_current();
        auto serialized_size = _current_buffer->write(m);
        uint32_t crc = crc32c::extend(type_crc_[record_type::full], record, serialized_size);
        crc = crc32c::mask(crc);
        // [0-3] 4bytes for crc
        encode_fixed32(header, crc);
        header[4] = static_cast<char>(serialized_size & 0xff);
        header[5] = static_cast<char>(serialized_size >> 8);
        header[6] = static_cast<char>(record_type::full);
    }).then([this] {
        _current_buffer->leave();
        _gate.leave();
        return make_ready_future<>();
    });
}

future<> commit_log::impl::close()
{
    return _gate.close().then([this] {
        // flush all pending buffers to disk;
    });
}

commit_log::commit_log(std::unique_ptr<impl> i) : _impl(std::move(i))
{
}

commit_log::~commit_log()
{
}

future<> commit_log::append(lw_shared_ptr<mutation> m)
{
    return _impl->append(m);
}

future<> commit_log::close()
{
    return _impl->close();
}

lw_shared_ptr<commit_log> make_commit_log()
{
    using clk = std::chrono::system_clock;
    auto timestap = clk::now().time_since_epoch().count();
    sstring filename = "commitlog-" + to_sstring(engine().cpu_id()) + "-" + to_sstring(timestap) + ".log";
    return make_lw_shared<commit_log>(std::make_unique<commit_log::impl>(std::move(filename)));
}
}
