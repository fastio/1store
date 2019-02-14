#pragma once
#include "bytes.hh"
#include "seastar/core/future.hh"
#include "seastar/core/sstring.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "mutation.hh"
#include "db/consistency_level_type.hh"
#include "db/timeout_clock.hh"
#include "keys.hh"
using namespace seastar;

class timeout_config;

namespace service {
class storage_proxy;
class client_state;
}

namespace tracing {
class trace_state_ptr;
}
namespace redis {


class abstract_command : public enable_shared_from_this<abstract_command> {
protected:
    bytes _name;
public:
    abstract_command(bytes&& name) : _name(std::move(name)) {}
    virtual ~abstract_command() {};
    virtual future<reply> execute(service::storage_proxy&, db::consistency_level cl, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& client_state) = 0;
    const bytes& name() const { return _name; }
    static sstring make_sstring(bytes b) {
        return sstring{reinterpret_cast<char*>(b.data()), b.size()};
    }
};

class mutation_helper {
public:
    static mutation make_mutation(schema_ptr schema, bytes& key) {
        auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(abstract_command::make_sstring(key)));
        return std::move(mutation(schema, std::move(pkey)));
    }
};
}
