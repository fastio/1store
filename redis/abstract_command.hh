#pragma once
#include "bytes.hh"
#include "seastar/core/future.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "mutation.hh"
#include "db/consistency_level_type.hh"
#include "db/timeout_clock.hh"
#include "keys.hh"
using namespace seastar;

namespace service {
class storage_proxy;
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
    virtual future<reply> execute(service::storage_proxy&, db::consistency_level cl, db::timeout_clock::time_point, tracing::trace_state_ptr) = 0;
    const bytes& name() const { return _name; }
};

class mutation_helper {
public:
    static mutation make_mutation(schema_ptr schema, bytes& key) {
        auto pkey = partition_key::from_bytes(key);
        return std::move(mutation(schema, std::move(pkey)));
    }
};
}
