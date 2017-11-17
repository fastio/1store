#include "partition.hh"
#include "utils/bytes.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"
#include "cache.hh"

class deleted_partition_impl : public partition_impl {
    cache_entry& _entry;
public:
    deleted_partition_impl(cache_entry& entry, version_type version) : partition_impl(data_type::deleted, version), _entry(entry) {}
    virtual size_t encode_to(data_output& output) const override {
        return 0;
    }

    virtual void decode_from(data_input& input) override {

    }

    virtual size_t estimate_serialized_size() const override {
        return 0;
    }
};

lw_shared_ptr<partition> make_deleted_partition(cache_entry& e, version_type version) {
    return make_lw_shared<partition>(std::make_unique<deleted_partition_impl>(e, version));
}

class string_partition_impl : public partition_impl {
    cache_entry& _entry;
public:
    string_partition_impl(cache_entry& e, version_type v)
        : partition_impl(data_type::bytes, v)
        , _entry(e)
    {
    }

    virtual size_t encode_to(data_output& output) const override {
        output.write(static_cast<unsigned>(_type))
              .write(_version);
        return estimate_serialized_size();
    }

    virtual void decode_from(data_input& input) override {

    }

    virtual size_t estimate_serialized_size() const override {
        using output = data_output;
        return output::serialized_size<unsigned>() +
            output::serialized_size(_version);
    }
};

lw_shared_ptr<partition> make_string_partition(cache_entry& e, version_type version) {
    return make_lw_shared<partition>(std::make_unique<string_partition_impl>(e, version));
}
