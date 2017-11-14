#pragma once
#include "utils/bytes.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"
#include "utils/data_input.hh"
#include "utils/data_output.hh"
#include <memory>
enum class mutation_type : unsigned {
    string,
    list,
    set,
    hash,
    zset,
    unknown,
    null,
};

using mutation_generation_type = size_t;

class mutation_impl {
protected:
    mutation_type _type;
    mutation_generation_type _gen;
    bytes _key;
    size_t _hash;
public:
    mutation_impl() = default;
    mutation_impl(const mutation_impl&) = default;
    mutation_impl& operator = (const mutation_impl&) = default;
    mutation_impl(mutation_type type, const bytes& key, size_t hash) : _type(type), _key(key), _hash(hash) {}
    mutation_type type() const { return _type; }
    virtual size_t encode_to(data_output& output) const = 0;
    virtual void decode_from(data_input& input) = 0;
    virtual size_t estimate_serialized_size() const = 0;
};

class mutation {
    std::unique_ptr<mutation_impl> _impl;
public:
    mutation(const mutation&) = delete;
    mutation& operator = (const mutation&) = delete;
    mutation(mutation&& o) : _impl(std::move(o._impl)) {} 
    mutation& operator = (mutation&& o) {
        if (this != &o) {
            _impl = std::move(o._impl);
        }
        return *this;
    }
    mutation() : mutation(nullptr) {}
    mutation(std::unique_ptr<mutation_impl> impl) : _impl(std::move(impl)) {}
    mutation_type type() const;
    void replace_if_newer(mutation&& p) {}
    bool empty() const { return false; }
    size_t encode_to(data_output& output) { return _impl->encode_to(output); }
    void decode_from(data_input& input) { _impl->decode_from(input); }
    size_t estimate_serialized_size() const { return _impl->estimate_serialized_size(); }
};

lw_shared_ptr<mutation> make_null_mutation();
lw_shared_ptr<mutation> make_removable_mutation(const bytes& key, const size_t hash);
lw_shared_ptr<mutation> make_string_mutation(const bytes& key, const size_t hash, const bytes& value, long expire, int flag);
