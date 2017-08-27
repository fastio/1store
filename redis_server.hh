/*
* Pedis is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* You may obtain a copy of the License at
*
*     http://www.gnu.org/licenses
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*  Copyright (c) 2016-2026, Peng Jian, pstack at 163.com.
*
*/
#pragma once

#include "core/reactor.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "service/migration_listener.hh"
#include "redis_storage_proxy.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "cql3/values.hh"
#include "redis_protocol.hh"
#include "auth/authenticator.hh"
#include "core/distributed.hh"
#include <seastar/core/semaphore.hh>
#include <memory>
#include <boost/intrusive/list.hpp>
#include <seastar/net/tls.hh>
#include <seastar/core/metrics_registration.hh>

namespace scollectd {

class registrations;

}

class database;

namespace cql_transport {

class redis_server {
private:
    std::vector<server_socket> _listeners;
    distributed<service::redis_storage_proxy>& _proxy;
    size_t _max_request_size;
    semaphore _memory_available;
    seastar::metrics::metric_groups _metrics;
private:
    uint64_t _connects = 0;
    uint64_t _connections = 0;
    uint64_t _requests_served = 0;
    uint64_t _requests_serving = 0;
public:
    redis_server(distributed<service::redis_storage_proxy>& proxy);
    future<> listen(ipv4_addr addr, std::shared_ptr<seastar::tls::credentials_builder> = {}, bool keepalive = false);
    future<> do_accepts(int which, bool keepalive, ipv4_addr server_addr);
    future<> stop();
private:
    class connection : public boost::intrusive::list_base_hook<> {
        friend class redis_server;
        redis_server& _server;
        distributed<service::redis_storage_proxy>& _proxy;
        ipv4_addr _server_addr;
        connected_socket _fd;
        input_stream<char> _in;
        output_stream<char> _out;
        seastar::gate _pending_requests_gate;
        future<> _ready_to_respond = make_ready_future<>();
        service::client_state _client_state;
        redis::redis_protocol _proto;
        unsigned _request_cpu = 0;

        enum class state : uint8_t {
            UNINITIALIZED, AUTHENTICATION, READY
        };

        enum class tracing_request_type : uint8_t {
            not_requested,
            no_write_on_close,
            write_on_close
        };

        state _state = state::UNINITIALIZED;
        ::shared_ptr<auth::authenticator::sasl_challenge> _sasl_challenge;
    public:
        connection(redis_server& server, distributed<service::redis_storage_proxy>& proxy, ipv4_addr server_addr, connected_socket&& fd, socket_address addr);
        ~connection();
        future<> process();
        future<> process_request();
        future<> shutdown();
    };

    friend class type_codec;
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
