#include "server.hh"

#include <boost/bimap/unordered_set_of.hpp>
#include <boost/range/irange.hpp>
#include <boost/bimap.hpp>
#include <boost/assign.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <boost/range/adaptor/sliced.hpp>

#include "service.hh"
#include "core/future-util.hh"
#include "core/reactor.hh"
#include "net/byteorder.hh"
#include <seastar/core/metrics.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/util/lazy.hh>
#include <seastar/core/execution_stage.hh>

#include "exceptions/exceptions.hh"
#include <cassert>
#include <string>

#include <snappy-c.h>
#include <lz4.h>

namespace redis {

static logging::logger rlog("server");

distributed<server> _the_server;

server::server(distributed<redis::proxy>& p)
    : _proxy(p)
    , _max_request_size(memory::stats().total_memory() / 10)
    , _memory_available(_max_request_size)
{
    namespace sm = seastar::metrics;

    _metrics.add_group("transport", {
        sm::make_derive("cql-connections", _connects,
                        sm::description("Counts a number of client connections.")),

        sm::make_gauge("current_connections", _connections,
                        sm::description("Holds a current number of client connections.")),

        sm::make_derive("requests_served", _requests_served,
                        sm::description("Counts a number of served requests.")),

        sm::make_gauge("requests_serving", _requests_serving,
                        sm::description("Holds a number of requests that are being processed right now.")),

        sm::make_gauge("requests_blocked_memory", [this] { return _memory_available.waiters(); },
                        sm::description(
                            seastar::format("Holds a number of requests that are blocked due to reaching the memory quota limit ({}B). "
                                            "Non-zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \" redis\" component.", _max_request_size))),
    });
}

future<> server::stop() {
    _stopping = true;
    size_t nr = 0;
    size_t nr_total = _listeners.size();
    rlog.debug("server: abort accept nr_total={}", nr_total);
    for (auto&& l : _listeners) {
        l.abort_accept();
        rlog.debug("server: abort accept {} out of {} done", ++nr, nr_total);
    }
    auto nr_conn = make_lw_shared<size_t>(0);
    auto nr_conn_total = _connections_list.size();
    rlog.debug("server: shutdown connection nr_total={}", nr_conn_total);
    return parallel_for_each(_connections_list.begin(), _connections_list.end(), [nr_conn, nr_conn_total] (auto&& c) {
        return c.shutdown().then([nr_conn, nr_conn_total] {
            rlog.debug("server: shutdown connection {} out of {} done", ++(*nr_conn), nr_conn_total);
        });
    }).then([this] {
        return std::move(_stopped);
    });
}

future<>
server::listen(ipv4_addr addr, std::shared_ptr<seastar::tls::credentials_builder> creds, bool keepalive) {
    listen_options lo;
    lo.reuse_address = true;
    server_socket ss;
    try {
        ss = creds
          ? seastar::tls::listen(creds->build_server_credentials(), make_ipv4_address(addr), lo)
          : engine().listen(make_ipv4_address(addr), lo);
    } catch (...) {
        throw std::runtime_error(sprint("redis server error while listening on %s -> %s", make_ipv4_address(addr), std::current_exception()));
    }
    _listeners.emplace_back(std::move(ss));
    _stopped = when_all(std::move(_stopped), do_accepts(_listeners.size() - 1, keepalive, addr)).discard_result();
    return make_ready_future<>();
}

future<>
server::do_accepts(int which, bool keepalive, ipv4_addr server_addr) {
    ++_connections_being_accepted;
    return _listeners[which].accept().then_wrapped([this, which, keepalive, server_addr] (future<connected_socket, socket_address> f_cs_sa) mutable {
        --_connections_being_accepted;
        if (_stopping) {
            f_cs_sa.ignore_ready_future();
            maybe_idle();
            return make_ready_future<>();
        }
        auto cs_sa = f_cs_sa.get();
        auto fd = std::get<0>(std::move(cs_sa));
        auto addr = std::get<1>(std::move(cs_sa));
        fd.set_nodelay(true);
        fd.set_keepalive(keepalive);
        auto conn = make_shared<connection>(*this, server_addr, std::move(fd), std::move(addr));
        ++_connects;
        ++_connections;
        conn->process().then_wrapped([this, conn] (future<> f) {
            --_connections;
            try {
                f.get();
            } catch (...) {
                rlog.debug("connection error: {}", std::current_exception());
            }
        });
        return do_accepts(which, keepalive, server_addr);
    }).then_wrapped([this, which, keepalive, server_addr] (future<> f) {
        try {
            f.get();
        } catch (...) {
            rlog.warn("acccept failed: {}", std::current_exception());
            return do_accepts(which, keepalive, server_addr);
        }
        return make_ready_future<>();
    });
}

server::connection::connection(server& server, ipv4_addr server_addr, connected_socket&& fd, socket_address addr)
    : _server(server)
    , _server_addr(server_addr)
    , _fd(std::move(fd))
    , _in(_fd.input())
    , _out(_fd.output())
{
    ++_server._total_connections;
    ++_server._current_connections;
    _server._connections_list.push_back(*this);
}

server::connection::~connection() {
    --_server._current_connections;
    _server._connections_list.erase(_server._connections_list.iterator_to(*this));
    _server.maybe_idle();
}

future<> server::connection::process()
{
    return do_until([this] { return _in.eof(); }, [this] {
        return with_gate(_pending_requests_gate, [this] {
            return process_request();
        });
    }).then_wrapped([this] (future<> f) {
        try {
            f.get();
            return make_ready_future<>();
        } catch (...) {
            throw request_exception("request error");
        }
    }).finally([this] {
        return _pending_requests_gate.close().then([this] {
            return _ready_to_respond.finally([this] {
                return _out.close();
            });
        });
    });
}

future<> server::connection::shutdown()
{
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
    }
    return make_ready_future<>();
}

future<> server::connection::process_request() {
    return make_ready_future<>();
}
}
