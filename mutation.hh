#pragma once
#include "utils/bytes.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"
#include "utils/data_input.hh"
#include "utils/data_output.hh"
#include "types.hh"
#include <memory>

using mutation_generation_type = size_t;

class mutation;
class mutation_impl {
protected:
    data_type _type;
    mutation_generation_type _gen;
    bytes _key;

    friend class mutation;
public:
    mutation_impl() = default;
    mutation_impl(const mutation_impl&) = default;
    mutation_impl& operator = (const mutation_impl&) = default;
    mutation_impl(data_type type, const bytes& key) : _type(type), _key(key) {}
    data_type type() const { return _type; }
    virtual size_t encode_to(data_output& output) const = 0;
    virtual void decode_from(data_input& input) = 0;
    virtual size_t estimate_serialized_size() const = 0;
    const bytes& key() const { return _key; }
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
    data_type type() const;
    bool empty() const { return false; }
    size_t encode_to(data_output& output) { return _impl->encode_to(output); }
    void decode_from(data_input& input) { _impl->decode_from(input); }
    size_t estimate_serialized_size() const { return _impl->estimate_serialized_size(); }
};

lw_shared_ptr<mutation> make_deleted_mutation(const bytes& key);
lw_shared_ptr<mutation> make_bytes_mutation(const bytes& key, const bytes& value, long expire, int flag);
