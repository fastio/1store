#pragma once
#include "redis/abstract_command.hh"
#include <vector>
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


class command_with_multi_schemas : public abstract_command {
protected:
    std::vector<schema_ptr> _schemas;
public:
    command_with_multi_schemas(bytes&& name, std::vector<schema_ptr>&& schemas)
        : abstract_command(std::move(name))
        , _schemas(std::move(schemas))
    {
    }
    virtual ~command_with_multi_schemas() {};
};

} // end of redis namespace
