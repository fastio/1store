//FIXME: Copyright header
#pragma once

#include "database.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "query-result-set.hh"
#include "core/distributed.hh"
#include "db/consistency_level.hh"
#include "db/write_type.hh"
#include "utils/histogram.hh"
#include "utils/estimated_histogram.hh"
#include "tracing/trace_state.hh"
#include <seastar/core/metrics.hh>
namespace compat {

class one_or_two_partition_ranges;

}
namespace dht {
    class decorated_key;
}
namespace redis {
    class redis_service;
    class request_wrapper;
    class result;
}

namespace service {

class redis_storage_proxy : public seastar::async_sharded_service<redis_storage_proxy> {
public:
    using clock_type = lowres_clock;
private:
    distributed<redis::redis_service> _redis;
    seastar::metrics::metric_groups _metrics;
private:
    void uninit_messaging_service() {}
    std::vector<gms::inet_address> get_live_endpoints(const dht::token& token);
    future<> proxy_command_to_endpoint(gms::inet_address addr, const redis::request_wrapper& req);
    dht::decorated_key construct_decorated_key_from(const bytes& key) const;

    future<foreign_ptr<lw_shared_ptr<redis::result>>> execute(const redis::request_wrapper& req);
    future<> execute_command_set(const redis::request_wrapper& req, output_stream<char>& out);
    future<> execute_command_get(const redis::request_wrapper& req, output_stream<char>& out);
    future<> execute_command_del(const redis::request_wrapper& req, output_stream<char>& out);

public:
    redis_storage_proxy();
    ~redis_storage_proxy();

    future<> execute(const redis::request_wrapper& req, output_stream<char>& out);
    void init_messaging_service();


    future<> stop();
};

extern distributed<redis_storage_proxy> _the_redis_storage_proxy;

inline distributed<redis_storage_proxy>& get_redis_storage_proxy() {
    return _the_redis_storage_proxy;
}

inline redis_storage_proxy& get_local_redis_storage_proxy() {
    return _the_redis_storage_proxy.local();
}

inline shared_ptr<redis_storage_proxy> get_local_shared_redis_storage_proxy() {
    return _the_redis_storage_proxy.local_shared();
}

}
