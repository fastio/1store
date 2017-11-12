#pragma once
#include "utils/bytes.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"
#include <memory>

enum class partition_type : unsigned {
    string,
    list,
    set,
    hash,
    zset,
    unknown,
    null,
};

using partition_generation_type = size_t;

class partition_impl {
protected:
    partition_type _type;
    partition_generation_type _gen;
    bytes _key;
    size_t _hash;
public:
    partition_impl() = default;
    partition_impl(const partition_impl&) = default;
    partition_impl& operator = (const partition_impl&) = default;
    partition_impl(partition_type type, const bytes& key, size_t hash) : _type(type), _key(key), _hash(hash) {}
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
    partition_type type() const;
    void replace_if_newer(partition&& p) {}
    bool empty() const { return false; }
    bytes serialize() const { return _impl->serialize(); }
};

lw_shared_ptr<partition> make_null_partition();
lw_shared_ptr<partition> make_removable_partition(const bytes& key, const size_t hash);
lw_shared_ptr<partition> make_string_partition(const bytes& key, const size_t hash, const bytes& value);
