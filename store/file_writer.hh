#pragma once
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
}
