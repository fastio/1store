#pragma once
#include "utils/bytes.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"
#include <memory>
enum class partition_type : unsigned {
    list,
    set,
    hash,
    zset,
    string,
    unknown,
};

using partition_generation_type = size_t;

class partition_impl {
    partition_type _type;
    partition_generation_type _gen;
    bytes _key;
public:
    partition_impl() = default;
    partition_impl(const partition_impl&) = default;
    partition_impl& operator = (const partition_impl&) = default;
    partition_impl(partition_type type, const bytes& key) : _type(type), _key(key) {}
    virtual bytes serialize() = 0;
    partition_type type() const { return _type; }
};

class partition {
    std::unique_ptr<partition_impl> _impl;
public:
    partition(const partition&) = delete;
    partition& operator = (const partition&) = delete;
    partition(partition&& o) : _impl(std::move(o._impl)) {} 
    partition& operator = (partition&& o) {
        if (this != &o) {
            _impl = std::move(o._impl);
        }
        return *this;
    }
    partition() : partition(nullptr) {}
    partition(std::unique_ptr<partition_impl> impl) : _impl(std::move(impl)) {}
    bytes serialize() const;
    partition_type type() const;
};

partition make_removable_partition(const bytes& key);
partition make_sstring_partition(const bytes& key, const bytes& value);
