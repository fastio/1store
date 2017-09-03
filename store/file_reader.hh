#pragma once
#include <vector>
#include <typeinfo>
#include <limits>
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/sstring.hh"
#include "core/fstream.hh"
#include "core/shared_ptr.hh"
#include "core/do_with.hh"
#include "core/thread.hh"
#include "core/align.hh"
#include "core/file.hh"
#include "core/sstring.hh"
#include "core/seastar.hh"
#include "core/shared_future.hh"
#include "core/byteorder.hh"
#include "core/gate.hh"
#include "utils/disk-error-handler.hh"
#include "utils/bytes.hh"
#include "store/checked-file-impl.hh"
#include <iterator>
#include "seastarx.hh"
#include "exceptions/exceptions.hh"

namespace store {
future<file> make_file(const io_error_handler& error_handler, sstring name, open_flags flags) {
    return open_checked_file_dma(error_handler, name, flags).handle_exception([name] (auto ep) {
        return make_exception_future<file>(ep);
    });
}

class random_access_reader {
    std::unique_ptr<input_stream<char>> _in;
    seastar::gate _close_gate;
protected:
    virtual input_stream<char> open_at(uint64_t pos) = 0;
public:
    future<temporary_buffer<char>> read_exactly(size_t n) {
        return _in->read_exactly(n);
    }
    void seek(uint64_t pos) {
        if (_in) {
            seastar::with_gate(_close_gate, [in = std::move(_in)] () mutable {
                auto fut = in->close();
                return fut.then([in = std::move(in)] {});
            });
        }
        _in = std::make_unique<input_stream<char>>(open_at(pos));
    }
    bool eof() { return _in->eof(); }
    virtual future<> close() {
        return _close_gate.close().then([this] {
            return _in->close();
        });
    }
    virtual ~random_access_reader() { }
};

class file_random_access_reader : public random_access_reader {
    file _file;
    uint64_t _file_size;
    size_t _buffer_size;
    unsigned _read_ahead;
public:
    virtual input_stream<char> open_at(uint64_t pos) override {
        auto len = _file_size - pos;
        file_input_stream_options options;
        options.buffer_size = _buffer_size;
        options.read_ahead = _read_ahead;

        return make_file_input_stream(_file, pos, len, std::move(options));
    }
    explicit file_random_access_reader(file f, uint64_t file_size, size_t buffer_size = 8192, unsigned read_ahead = 4)
        : _file(std::move(f)), _file_size(file_size), _buffer_size(buffer_size), _read_ahead(read_ahead)
    {
        seek(0);
    }
    virtual future<> close() override {
        return random_access_reader::close().finally([this] {
            return _file.close().handle_exception([save = _file] (auto ep) {
                general_disk_error();
            });
        });
    }
};

struct read_file_options {
    size_t _buffer_size = 8192;
    unsigned _read_ahead = 4;
    const io_priority_class& _io_priority_class;
    read_file_options(const io_priority_class& pc) : _io_priority_class(pc) {}
    read_file_options(read_file_options&& o)
        : _buffer_size (std::move(o._buffer_size))
        , _read_ahead(std::move(o._read_ahead))
        , _io_priority_class(o._io_priority_class)
    {
    }
};

template <typename T>
future<> read_file(const sstring& filename, T& component, const io_error_handler& handler, read_file_options&& opt) {
    auto file_path = filename;
    return open_file_dma(file_path, open_flags::ro).then([&component, opt = std::move(opt), &handler] (file file_) {
        auto fut = file_.size();
        return fut.then([&component, file_ = std::move(file_), opt = std::move(opt), &handler] (uint64_t size) {
             auto f = make_checked_file(handler, file_);
             auto r = make_lw_shared<file_random_access_reader>(std::move(f), size, opt._buffer_size);
             auto fut = decode_from(*r, component);
             return fut.finally([r] {
                 return r->close();
             }).then([r] {});
        });
    }).then_wrapped([file_path] (future<> f) {
        try {
            f.get();
        } catch (std::system_error& e) {
            if (e.code() == std::error_code(ENOENT, std::system_category())) {
                throw redis::io_exception(file_path + ": file not found");
            }
            throw;
        }
        return make_ready_future<>();
    });
}
}
