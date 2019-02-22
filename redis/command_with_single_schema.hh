#pragma once
#include "redis/abstract_command.hh"
using namespace seastar;

class timeout_config;

namespace service {
//class storage_proxy;
class client_state;
}

namespace cql3 {
class query_options;
}

namespace tracing {
class trace_state_ptr;
}

namespace redis {


class command_with_single_schema : public abstract_command {
protected:
    const schema_ptr _schema;
public:
    command_with_single_schema(bytes&& name, const schema_ptr schema, const gc_clock::duration ttl) : abstract_command(std::move(name), ttl), _schema(schema) {}
    command_with_single_schema(bytes&& name, const schema_ptr schema)
        : abstract_command(std::move(name), schema->default_time_to_live()), _schema(schema)
    {
    }
    virtual ~command_with_single_schema() {};
};

} // end of redis namespace
