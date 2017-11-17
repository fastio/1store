#include "mutation.hh"
#include "utils/bytes.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"


data_type mutation::type() const
{
    return _impl->type();
}

class deleted_mutation_impl : public mutation_impl {
public:
    deleted_mutation_impl(const bytes& key) : mutation_impl(data_type::deleted, key) {}
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

lw_shared_ptr<mutation> make_deleted_mutation(const bytes& key) {
    return make_lw_shared<mutation>(std::make_unique<deleted_mutation_impl>(key));
}

class string_mutation_impl : public mutation_impl {
    bytes _value;
    long _expire;
    int _flag;
public:
    string_mutation_impl(const bytes& key, const bytes& value, long expire, int flag)
        : mutation_impl(data_type::bytes, key)
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

lw_shared_ptr<mutation> make_bytes_mutation(const bytes& key, const bytes& value, long expire, int flag) {
    return make_lw_shared<mutation>(std::make_unique<string_mutation_impl>(key, value, expire, flag));
}
