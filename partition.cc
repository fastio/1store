#include "partition.hh"
#include "utils/bytes.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"


partition_type partition::type() const
{
    return _impl->type();
}

class null_partition_impl : public partition_impl {
public:
    null_partition_impl() : partition_impl(partition_type::null, {}, 0) {}
    virtual bytes serialize() override {
        return {};
    }
};

lw_shared_ptr<partition> make_null_partition() {
    return make_lw_shared<partition>(std::make_unique<null_partition_impl>());
}

class removable_partition_impl : public partition_impl {
public:
    removable_partition_impl(const bytes& key, const size_t hash) : partition_impl(partition_type::unknown, key, hash) {}
    virtual bytes serialize() override {
        return _key;
    }
};

lw_shared_ptr<partition> make_removable_partition(const bytes& key, size_t hash) {
    return make_lw_shared<partition>(std::make_unique<removable_partition_impl>(key, hash));
}

class string_partition_impl : public partition_impl {
   bytes _value;
public:
   string_partition_impl(const bytes& key, size_t hash, const bytes& value)
       : partition_impl(partition_type::string, key, hash)
       , _value(value)
   {
   }
   virtual bytes serialize() override {
       return _value;
   }
};

lw_shared_ptr<partition> make_string_partition(const bytes& key, const size_t hash, const bytes& value) {
    return make_lw_shared<partition>(std::make_unique<string_partition_impl>(key, hash, value));
}
