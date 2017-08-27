//FIXME: Copyright header
#include "partition_range_compat.hh"
#include "db/consistency_level.hh"
#include "db/commitlog/commitlog.hh"
#include "redis_storage_proxy.hh"
#include "unimplemented.hh"
#include "frozen_mutation.hh"
#include "query_result_merger.hh"
#include "core/do_with.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"
#include "core/future-util.hh"
#include "db/read_repair_decision.hh"
#include "db/config.hh"
#include "db/batchlog_manager.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/cxx11/none_of.hpp>
#include <boost/range/algorithm/count_if.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/numeric.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/empty.hpp>
#include <boost/range/algorithm/min_element.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "utils/latency.hh"
#include "schema.hh"
#include "schema_registry.hh"
#include "utils/joinpoint.hh"
#include <seastar/util/lazy.hh>
#include "core/metrics.hh"
#include <core/execution_stage.hh>
#include "redis_service.hh"
#include "request_wrapper.hh"
#include "result.hh"
#include "reply_builder.hh"
#include "redis_command_code.hh"

namespace service {
using namespace seastar;
static logging::logger slogger("redis_storage_proxy");
static logging::logger qlogger("query_result");
static logging::logger mlogger("mutation_data");

distributed<service::redis_storage_proxy> _the_redis_storage_proxy;

using namespace exceptions;

static inline bool is_me(gms::inet_address from) {
    return from == utils::fb_utilities::get_broadcast_address();
}

static inline sstring get_dc(gms::inet_address ep) {
    auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
    return snitch_ptr->get_datacenter(ep);
}

static inline sstring get_local_dc() {
    auto local_addr = utils::fb_utilities::get_broadcast_address();
    return get_dc(local_addr);
}

redis_storage_proxy::redis_storage_proxy() {

}

redis_storage_proxy::~redis_storage_proxy() {
}

future<>
redis_storage_proxy::stop() {
    uninit_messaging_service();
    return make_ready_future<>();
}

std::vector<gms::inet_address> redis_storage_proxy::get_live_endpoints(const dht::token& token) {
/*
    auto& ks = get_redis_keyspace();
    auto& rs = ks.get_replication_strategy();
    std::vector<gms::inet_address> eps = rs.get_natural_endpoints(token);
    auto itend = boost::range::remove_if(eps, std::not1(std::bind1st(std::mem_fn(&gms::failure_detector::is_alive), &gms::get_local_failure_detector())));
    eps.erase(itend, eps.end());
    return std::move(eps);
*/
    return {};
}

future<> redis_storage_proxy::proxy_command_to_endpoint(gms::inet_address addr, const redis::request_wrapper& req) {
    return make_ready_future<>();
}
    
dht::decorated_key redis_storage_proxy::construct_decorated_key_from(const bytes& key) const {
    auto& partitioner = dht::global_partitioner();
    return { partitioner.get_token(key), std::move(partition_key::from_bytes(key)) };
}

future<> redis_storage_proxy::execute_command_set(const redis::request_wrapper& req, output_stream<char>& out)
{
    /*
    if (req._args_count != 2) {
        return redis::reply_builder::build_invalid_argument_message(out);
    }
    auto dk = construct_decorated_key_from(req._args[0]);
    auto targets = get_live_endpoints(dk.token());
    if (targets.empty()) {
        return redis::reply_builder::build_system_error_message(out);
    }
    if (is_me(targets[0])) {
        return get_local_redis_service().set(req, out);
    }
    return proxy_command_to_endpoint(targets[0], req).then([this, &out] (auto result) {
        return redis::reply_builder::build(result, out);
    });
    */
    return make_ready_future<>();
}

future<> redis_storage_proxy::execute_command_get(const redis::request_wrapper& req, output_stream<char>& out)
{
    return make_ready_future<>();
}

future<> redis_storage_proxy::execute_command_del(const redis::request_wrapper& req, output_stream<char>& out)
{
    return make_ready_future<>();
}

future<> redis_storage_proxy::execute(const redis::request_wrapper& req, output_stream<char>& out) {
    switch (req._command) {
        case redis::command_code::set: return execute_command_set(req, out);
        case redis::command_code::get: return execute_command_get(req, out);
        case redis::command_code::del: return execute_command_del(req, out);
        default:
            return make_ready_future<>();
    }
}
}
