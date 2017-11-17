#pragma once
#include "utils/bytes.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"
#include "utils/data_input.hh"
#include "utils/data_output.hh"
#include "types.hh"
#include <memory>
class partition;
using version_type = uint64_t;

class partition_impl {
protected:
    data_type _type;
    version_type _version;
    friend class partition;
public:
    partition_impl() = default;
    partition_impl(const partition_impl&) = default;
    partition_impl& operator = (const partition_impl&) = default;
    partition_impl(data_type type, version_type version) : _type(type), _version(version) {}
    data_type type() const { return _type; }
    version_type version() const { return _version; }
    virtual size_t encode_to(data_output& output) const = 0;
    virtual void decode_from(data_input& input) = 0;
    virtual size_t estimate_serialized_size() const = 0;
};

class partition {
    std::unique_ptr<partition_impl> _impl;
public:
    partition(const partition&) = delete;
    partition& operator = (const partition&) = delete;
    partition(partition&& o) noexcept : _impl(std::move(o._impl)) {} 
    partition& operator = (partition&& o) noexcept {
        if (this != &o) {
            _impl = std::move(o._impl);
        }
        return *this;
    }
    partition() : partition(nullptr) {}
    partition(std::unique_ptr<partition_impl> impl) : _impl(std::move(impl)) {}
    data_type type() const { return _impl->type(); }
    version_type version() const { return _impl->version(); }
    size_t encode_to(data_output& output) { return _impl->encode_to(output); }
    void decode_from(data_input& input) { _impl->decode_from(input); }
    size_t estimate_serialized_size() const { return _impl->estimate_serialized_size(); }
};

class cache_entry;
lw_shared_ptr<partition> make_deleted_partition(cache_entry&, version_type);
lw_shared_ptr<partition> make_bytes_partition(cache_entry&, version_type);
