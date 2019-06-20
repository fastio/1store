#include "redis/commands/zadd.hh"
#include "redis/commands/unexpected.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "db/system_keyspace.hh"
#include "types.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "mutation.hh"
#include "timeout_config.hh"
#include "redis/redis_mutation.hh"
#include <boost/lexical_cast.hpp>
//#include "log.hh"
namespace redis {

namespace commands {

shared_ptr<abstract_command> zadd::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 3) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    std::vector<std::pair<bytes, bytes>> data;
    for (size_t i = 1; i < req._args_count; i += 2) {
        /*
        try {
            auto& score = req._args[ i ];
            boost::lexical_cast<double>(score);
        } catch (boost::bad_lexical_cast&) {
        }
        */
        if (is_number(req._args[i]) == false) {
            //value is not a valid float
            return unexpected::make_exception(std::move(req._command), sstring("-ERR value is not a valid float\r\n"));
        }
        data.emplace_back(std::make_pair(req._args[i + 1], req._args[i]));
    }
    return seastar::make_shared<zadd> (std::move(req._command), zsets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(data));
}

future<redis_message> zadd::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    auto total = _data.size();
    return redis::write_mutation(proxy, redis::make_zset_cells(_schema, _key, std::move(_data)), cl, timeout, cs).then_wrapped([this, total] (auto f) {
        try {
            f.get();
        } catch (std::exception& e) {
            return redis_message::err();
        }
        return redis_message::make_long(static_cast<long>(total));
    });
}

}
}
