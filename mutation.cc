#include "mutation.hh"
#include "utils/bytes.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"


mutation_type mutation::type() const
{
    return _impl->type();
}

class null_mutation_impl : public mutation_impl {
public:
    null_mutation_impl() : mutation_impl(mutation_type::null, {}, 0) {}
    virtual size_t encode_to(data_output& output) const override {
        return 0;
    }

    virtual void decode_from(data_input& input) override {

    }
};

lw_shared_ptr<mutation> make_null_mutation() {
    return make_lw_shared<mutation>(std::make_unique<null_mutation_impl>());
}

class removable_mutation_impl : public mutation_impl {
public:
    removable_mutation_impl(const bytes& key, const size_t hash) : mutation_impl(mutation_type::unknown, key, hash) {}
    virtual size_t encode_to(data_output& output) const override {
        return 0;
    }

    virtual void decode_from(data_input& input) override {

    }
};

lw_shared_ptr<mutation> make_removable_mutation(const bytes& key, size_t hash) {
    return make_lw_shared<mutation>(std::make_unique<removable_mutation_impl>(key, hash));
}

class string_mutation_impl : public mutation_impl {
    bytes _value;
public:
    string_mutation_impl(const bytes& key, size_t hash, const bytes& value)
        : mutation_impl(mutation_type::string, key, hash)
        , _value(value)
    {
    }

    virtual size_t encode_to(data_output& output) const override {
        return 0;
    }

    virtual void decode_from(data_input& input) override {

    }
};

lw_shared_ptr<mutation> make_string_mutation(const bytes& key, const size_t hash, const bytes& value) {
    return make_lw_shared<mutation>(std::make_unique<string_mutation_impl>(key, hash, value));
}
