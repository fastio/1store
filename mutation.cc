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
        output.write(static_cast<unsigned>(_type))
              .write(_gen)
              .write(_key);
        return estimate_serialized_size();
    }

    virtual void decode_from(data_input& input) override {

    }

    virtual size_t estimate_serialized_size() const override {
        using output = data_output;
        return output::serialized_size<unsigned>() +
            output::serialized_size(_gen) +
            output::serialized_size(_key);
    }
};

lw_shared_ptr<mutation> make_null_mutation() {
    return make_lw_shared<mutation>(std::make_unique<null_mutation_impl>());
}

class removable_mutation_impl : public mutation_impl {
public:
    removable_mutation_impl(const bytes& key, const size_t hash) : mutation_impl(mutation_type::unknown, key, hash) {}
    virtual size_t encode_to(data_output& output) const override {
        output.write(static_cast<unsigned>(_type))
              .write(_gen)
              .write(_key);
        return estimate_serialized_size();
    }

    virtual void decode_from(data_input& input) override {

    }

    virtual size_t estimate_serialized_size() const override {
        using output = data_output;
        return output::serialized_size<unsigned>() +
            output::serialized_size(_gen) +
            output::serialized_size(_key);
    }
};

lw_shared_ptr<mutation> make_removable_mutation(const bytes& key, size_t hash) {
    return make_lw_shared<mutation>(std::make_unique<removable_mutation_impl>(key, hash));
}

class string_mutation_impl : public mutation_impl {
    bytes _value;
    long _expire;
    int _flag;
public:
    string_mutation_impl(const bytes& key, size_t hash, const bytes& value, long expire, int flag)
        : mutation_impl(mutation_type::string, key, hash)
        , _value(value)
        , _expire(expire)
        , _flag(flag)
    {
    }

    virtual size_t encode_to(data_output& output) const override {
        output.write(static_cast<unsigned>(_type))
              .write(_gen)
              .write(_key)
              .write(_value)
              .write(_expire)
              .write(_flag);
        return estimate_serialized_size();
    }

    virtual void decode_from(data_input& input) override {

    }

    virtual size_t estimate_serialized_size() const override {
        using output = data_output;
        return output::serialized_size<unsigned>() +
            output::serialized_size(_gen) +
            output::serialized_size(_key) +
            output::serialized_size(_value) +
            output::serialized_size(_expire) +
            output::serialized_size(_flag);
    }
};

lw_shared_ptr<mutation> make_string_mutation(const bytes& key, const size_t hash, const bytes& value, long expire, int flag) {
    return make_lw_shared<mutation>(std::make_unique<string_mutation_impl>(key, hash, value, expire, flag));
}
