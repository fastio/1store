#include "partition.hh"
#include "utils/bytes.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"


bytes partition::serialize() const
{
    return _impl->serialize();
}

partition_type partition::type() const
{
    return _impl->type();
}

class null_partition_impl : public partition_impl {
public:
    removable_partition_impl() : partition(partition_type::null, {}) {}
    virtual bytes serialize() override {
        return {};
    }
};

partition make_null_partition() {
    return partition(std::unique_ptr<null_partition_impl>());
}

class removable_partition_impl : public partition_impl {
public:
    removable_partition_impl(const bytes& key) : partition(partition_type::unknown, key) {}
    virtual bytes serialize() override {
        return _key;
    }
};

partition make_removable_partition(const bytes& key) {
    return partition(std::unique<removable_partition_impl>(key));
}

class string_partition_impl : public partition_impl {
   bytes _value;
public:
   string_partition_impl(const bytes& key, const bytes& value)
       : partition_impl(partition_type::string, key)
       , _value(value)
   {
   }
   virtual bytes serialize() override {
       return _value;
   }
};

partition make_sstring_partition(const bytes& key, const bytes& value) {
    return partition(std::unique<string_partition>(key, value));
}
