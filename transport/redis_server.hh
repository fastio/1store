/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "auth/service.hh"
#include "core/reactor.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "service/migration_listener.hh"
#include "service/storage_proxy.hh"
#include "redis/query_processor.hh"
#include "redis/request.hh"
#include "cql3/values.hh"
#include "auth/authenticator.hh"
#include "core/distributed.hh"
#include "timeout_config.hh"
#include <seastar/core/semaphore.hh>
#include <memory>
#include <boost/intrusive/list.hpp>
#include <seastar/net/tls.hh>
#include <seastar/core/metrics_registration.hh>
#include "utils/fragmented_temporary_buffer.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "redis/protocol_parser.hh"
class database;

namespace redis_transport {

enum class redis_load_balance {
    none,
    round_robin,
};

redis_load_balance parse_load_balance(sstring value);

struct redis_server_config {
    ::timeout_config timeout_config;
    size_t max_request_size;
};

class redis_server {
    std::vector<server_socket> _listeners;
    distributed<service::storage_proxy>& _proxy;
    distributed<redis::query_processor>& _query_processor;
    redis_server_config _config;
    size_t _max_request_size;
    semaphore _memory_available;
    seastar::metrics::metric_groups _metrics;
private:
    uint64_t _connects = 0;
    uint64_t _connections = 0;
    uint64_t _requests_served = 0;
    uint64_t _requests_serving = 0;
    uint64_t _requests_blocked_memory = 0;
    redis_load_balance _lb;
    auth::service& _auth_service;
public:
    redis_server(distributed<service::storage_proxy>& proxy, distributed<redis::query_processor>& qp, redis_load_balance lb, auth::service& auth_service, redis_server_config config);
    future<> listen(ipv4_addr addr, std::shared_ptr<seastar::tls::credentials_builder> = {}, bool keepalive = false);
    future<> do_accepts(int which, bool keepalive, ipv4_addr server_addr);
    future<> stop();
public:
    struct result {
        result(redis::redis_message&& m) : _data(make_foreign(std::make_unique<redis::redis_message>(std::move(m)))) {}
        foreign_ptr<std::unique_ptr<redis::redis_message>> _data;
        inline lw_shared_ptr<scattered_message<char>> make_message()  {
            return _data->message();
        }
    };
    using response_type = result;
private:
    class fmt_visitor;
    friend class connection;
    class connection : public boost::intrusive::list_base_hook<> {
        using result = redis_server::result;
        redis_server& _server;
        ipv4_addr _server_addr;
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        redis::protocol_parser _parser;
        seastar::gate _pending_requests_gate;
        service::client_state _client_state;
        future<> _ready_to_respond = make_ready_future<>();
        unsigned _request_cpu = 0;
    private:
        enum class tracing_request_type : uint8_t {
            not_requested,
            no_write_on_close,
            write_on_close
        };  

        using execution_stage_type = inheriting_concrete_execution_stage<
                future<redis_server::connection::result>,
                redis_server::connection*,
                redis::request&&,
                service::client_state,
                tracing_request_type
        >;
        static thread_local execution_stage_type _process_request_stage;
    public:
        connection(redis_server& server, ipv4_addr server_addr, connected_socket&& fd, socket_address addr);
        ~connection();
        future<> process();
        future<> process_request();
        future<> shutdown();
    private:
        const ::timeout_config& timeout_config() { return _server.timeout_config(); }
        friend class process_request_executor;
        future<result> process_request_one(redis::request&& request,  service::client_state cs, tracing_request_type rt);
        int maybe_change_keyspace(const redis::request& request, tracing_request_type rt);
        unsigned pick_request_cpu();
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
    const ::timeout_config& timeout_config() { return _config.timeout_config; }
};
}
