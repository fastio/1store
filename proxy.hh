#pragma once
#include "core/distributed.hh"
#include "utils/histogram.hh"
#include "utils/estimated_histogram.hh"
#include <seastar/core/metrics.hh>
#include "gms/inet_address.hh"
#include "keys.hh"
#include "token.hh"
namespace redis {
class service;
class request_wrapper;
class result;
class proxy;
extern distributed<proxy> _the_redis_proxy;

inline distributed<proxy>& get_proxy() {
    return _the_redis_proxy;
}

inline proxy& get_local_proxy() {
    return _the_redis_proxy.local();
}

inline shared_ptr<proxy> get_local_shared_proxy() {
    return _the_redis_proxy.local_shared();
}
class proxy : public seastar::async_sharded_service<proxy> {
public:
    using clock_type = lowres_clock;
private:
    distributed<redis::service> _redis;
    seastar::metrics::metric_groups _metrics;
private:
    void uninit_messaging_service() {}
    std::vector<gms::inet_address> get_live_endpoints(const redis::token& token);
    future<> proxy_command_to_endpoint(gms::inet_address addr, const redis::request_wrapper& req);

    future<foreign_ptr<lw_shared_ptr<redis::result>>> execute(const redis::request_wrapper& req);
    future<> execute_command_set(const redis::request_wrapper& req, output_stream<char>& out);
    future<> execute_command_get(const redis::request_wrapper& req, output_stream<char>& out);
    future<> execute_command_del(const redis::request_wrapper& req, output_stream<char>& out);

public:
    proxy() {}
    ~proxy() {}

    future<> execute(const redis::request_wrapper& req, output_stream<char>& out);
    void init_messaging_service();


    future<> stop();
};
}
