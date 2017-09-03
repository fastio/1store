#pragma once
#include "core/future.hh"
#include "core/file.hh"
#include "core/fstream.hh"
#include "core/iostream.hh"
#include "core/sstring.hh"
#include "core/seastar.hh"

#include "utils/disk-error-handler.hh"
#include "utils/bytes.hh"
#include "store/checked-file-impl.hh"
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/numeric.hpp>

#include "seastarx.hh"
namespace store {
class file_writer {
    output_stream<char> _out;
    size_t _offset = 0;
public:
    file_writer(file f, file_output_stream_options options)
        : _out(make_file_output_stream(std::move(f), std::move(options))) {}

    file_writer(output_stream<char>&& out)
        : _out(std::move(out)) {}

    virtual ~file_writer() = default;
    file_writer(file_writer&&) = default;

    future<> write(const char* buf, size_t n) {
        _offset += n;
        return _out.write(buf, n);
    }
    future<> write(const bytes& s) {
        _offset += s.size();
        return _out.write(s);
    }
    future<> flush() {
        return _out.flush();
    }
    future<> close() {
        return _out.close();
    }
    size_t offset() {
        return _offset;
    }
};

class sizing_data_sink : public data_sink_impl {
    uint64_t& _size;
public:
    explicit sizing_data_sink(uint64_t& dest) : _size(dest) {
        _size = 0;
    }
    virtual temporary_buffer<char> allocate_buffer(size_t size) {
        return temporary_buffer<char>(size);
    }
    virtual future<> put(net::packet data) override {
        _size += data.len();
        return make_ready_future<>();
    }
    virtual future<> put(std::vector<temporary_buffer<char>> data) override {
        _size += boost::accumulate(data | boost::adaptors::transformed(std::mem_fn(&temporary_buffer<char>::size)), 0);
        return make_ready_future<>();
    }
    virtual future<> put(temporary_buffer<char> buf) override {
        _size += buf.size();
        return make_ready_future<>();
    }
    virtual future<> flush() override {
        return make_ready_future<>();
    }
    virtual future<> close() override {
        return make_ready_future<>();
    }
};
inline
output_stream<char>
make_sizing_output_stream(uint64_t& dest) {
    return output_stream<char>(data_sink(std::make_unique<sizing_data_sink>(std::ref(dest))), 4096);
}

// Must be called from a thread
template <typename T>
uint64_t
serialized_size(const T& object) {
    uint64_t size = 0;
    auto writer = file_writer(make_sizing_output_stream(size));
    write(writer, object);
    writer.flush().get();
    writer.close().get();
    return size;
}

future<file> make_file(const io_error_handler& error_handler, sstring name, open_flags flags) {
    return open_checked_file_dma(error_handler, name, flags).handle_exception([name] (auto ep) {
        return make_exception_future<file>(ep);
    });
}

future<file> make_file(const io_error_handler& error_handler, sstring name, open_flags flags, file_open_options options) {
    return open_checked_file_dma(error_handler, name, flags, options).handle_exception([name] (auto ep) {
        return make_exception_future<file>(ep);
    });
}

struct write_file_options {
    size_t _buffer_size = 8192;
    const io_priority_class& _io_priority_class;
    write_file_options(const io_priority_class& pc) : _io_priority_class(pc) {}
    write_file_options(write_file_options&& o)
        : _buffer_size (std::move(o._buffer_size))
        , _io_priority_class(o._io_priority_class)
    {
    }
};

template <typename T>
future<> write_file(const sstring& fn, const T& component, const io_error_handler& handler, write_file_options&& opt) {
    auto file_path = fn;
    file f = make_file(handler, file_path, open_flags::wo | open_flags::create | open_flags::exclusive).get0();
    file_output_stream_options fopt;
    fopt.buffer_size = opt._buffer_size;
    fopt.io_priority_class = opt._io_priority_class;
    auto w = file_writer(std::move(f), std::move(fopt));
    encode_to(w, component);
    w.flush().get();
    w.close().get();
    return make_ready_future<>();
}
}
