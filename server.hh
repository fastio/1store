#pragma once

#include "proxy.hh"
#include "db.hh"
#include "redis_protocol.hh"
#include <memory>
#include <boost/intrusive/list.hpp>
#include "core/reactor.hh"
#include "core/distributed.hh"
#include "core/semaphore.hh"
#include "core/gate.hh"
#include "net/tls.hh"
#include "core/metrics_registration.hh"
#include "seastarx.hh"

namespace redis {

class server;
extern distributed<server> _the_server;
inline distributed<server>& get_server() {
    return _the_server;
}

class server {
private:
    std::vector<server_socket> _listeners;
    distributed<redis::proxy>& _proxy;
    size_t _max_request_size;
    semaphore _memory_available;
    seastar::metrics::metric_groups _metrics;
private:
    uint64_t _connects = 0;
    uint64_t _connections = 0;
    uint64_t _requests_served = 0;
    uint64_t _requests_serving = 0;
public:
    server(distributed<redis::proxy>& p);
    future<> listen(ipv4_addr addr, std::shared_ptr<seastar::tls::credentials_builder> = {}, bool keepalive = false);
    future<> do_accepts(int which, bool keepalive, ipv4_addr server_addr);
    future<> stop();
public:
private:
    class connection : public boost::intrusive::list_base_hook<> {
        server& _server;
        ipv4_addr _server_addr;
        connected_socket _fd;
        input_stream<char> _in;
        output_stream<char> _out;
        seastar::gate _pending_requests_gate;
        future<> _ready_to_respond = make_ready_future<>();
        unsigned _request_cpu = 0;
        redis_protocol _protocol;
    public:
        connection(server& server, ipv4_addr server_addr, connected_socket&& fd, socket_address addr);
        ~connection();
        future<> process();
        future<> process_request();
        future<> shutdown();
    };
private:
    bool _stopping = false;
    promise<> _all_connections_stopped;
    future<> _stopped = _all_connections_stopped.get_future();
    boost::intrusive::list<connection> _connections_list;
    uint64_t _total_connections = 0;
    uint64_t _current_connections = 0;
    uint64_t _connections_being_accepted = 0;
private:
    void maybe_idle() {
        if (_stopping && !_connections_being_accepted && !_current_connections) {
            _all_connections_stopped.set_value();
        }
    }
};
}
