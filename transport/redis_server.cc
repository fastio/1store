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

#include "redis_server.hh"

#include <boost/bimap/unordered_set_of.hpp>
#include <boost/range/irange.hpp>
#include <boost/bimap.hpp>
#include <boost/assign.hpp>
#include <boost/range/adaptor/sliced.hpp>

#include "cql3/statements/batch_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "db/consistency_level_type.hh"
#include "db/write_type.hh"
#include "core/future-util.hh"
#include "core/reactor.hh"
#include "utils/UUID.hh"
#include "net/byteorder.hh"
#include <seastar/core/metrics.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/util/lazy.hh>
#include <seastar/core/execution_stage.hh>

#include "enum_set.hh"
#include "service/query_state.hh"
#include "exceptions/exceptions.hh"

#include "auth/authenticator.hh"

#include <cassert>
#include <string>

#include <snappy-c.h>
#include <lz4.h>

#include "response.hh"
#include "request.hh"
#include "redis/reply.hh"
namespace redis_transport {

static logging::logger logging("redis_server");

struct redis_protocol_execption : std::exception {
    const char* what() const throw () override {
        return "bad cql binary frame";
    }
};

redis_load_balance parse_load_balance(sstring value)
{
    if (value == "none") {
        return redis_load_balance::none;
    } else if (value == "round-robin") {
        return redis_load_balance::round_robin;
    } else {
        throw std::invalid_argument("Unknown load balancing algorithm: " + value);
    }
}

redis_server::redis_server(distributed<service::storage_proxy>& proxy, distributed<redis::query_processor>& qp, redis_load_balance lb, auth::service& auth_service, redis_server_config config)
    : _proxy(proxy)
    , _query_processor(qp)
    , _config(config)
    , _max_request_size(config.max_request_size)
    , _memory_available(_max_request_size)
    , _lb(lb)
    , _auth_service(auth_service)
{
    namespace sm = seastar::metrics;
}

future<> redis_server::stop() {
    _stopping = true;
    size_t nr = 0;
    size_t nr_total = _listeners.size();
    logging.debug("redis_server: abort accept nr_total={}", nr_total);
    for (auto&& l : _listeners) {
        l.abort_accept();
        logging.debug("redis_server: abort accept {} out of {} done", ++nr, nr_total);
    }
    auto nr_conn = make_lw_shared<size_t>(0);
    auto nr_conn_total = _connections_list.size();
    logging.debug("redis_server: shutdown connection nr_total={}", nr_conn_total);
    return parallel_for_each(_connections_list.begin(), _connections_list.end(), [nr_conn, nr_conn_total] (auto&& c) {
        return c.shutdown().then([nr_conn, nr_conn_total] {
            logging.debug("redis_server: shutdown connection {} out of {} done", ++(*nr_conn), nr_conn_total);
        });
    }).then([this] {
        return std::move(_stopped);
    });
}

future<>
redis_server::listen(ipv4_addr addr, std::shared_ptr<seastar::tls::credentials_builder> creds, bool keepalive) {
    listen_options lo;
    lo.reuse_address = true;
    server_socket ss;
    try {
        ss = creds
          ? seastar::tls::listen(creds->build_server_credentials(), make_ipv4_address(addr), lo)
          : engine().listen(make_ipv4_address(addr), lo);
    } catch (...) {
        throw std::runtime_error(sprint("Redis server error while listening on %s -> %s", make_ipv4_address(addr), std::current_exception()));
    }
    _listeners.emplace_back(std::move(ss));
    _stopped = when_all(std::move(_stopped), do_accepts(_listeners.size() - 1, keepalive, addr)).discard_result();
    return make_ready_future<>();
}

future<>
redis_server::do_accepts(int which, bool keepalive, ipv4_addr server_addr) {
    return repeat([this, which, keepalive, server_addr] {
        ++_connections_being_accepted;
        return _listeners[which].accept().then_wrapped([this, which, keepalive, server_addr] (future<connected_socket, socket_address> f_cs_sa) mutable {
            --_connections_being_accepted;
            if (_stopping) {
                f_cs_sa.ignore_ready_future();
                maybe_idle();
                return stop_iteration::yes;
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
                    logging.debug("connection error: {}", std::current_exception());
                }
            });
            return stop_iteration::no;
        }).handle_exception([] (auto ep) {
            logging.debug("accept failed: {}", ep);
            return stop_iteration::no;
        });
    });
}

future<redis_server::connection::result> redis_server::connection::process_request_one(redis::request&& request,  service::client_state cs, tracing_request_type rt) {
    return futurize_apply([this, request = std::move(request), cs = std::move(cs)] () mutable {
        auto& config = _server._config.timeout_config;
        return _server._query_processor.local().process(std::move(request), cs, config).then([] (auto&& message) {
            return make_ready_future<redis_server::connection::result> (std::move(message));
        });
    });
}

int redis_server::connection::maybe_change_keyspace(const redis::request& req, tracing_request_type rt) {
    if (req._command != "select") {
        return -1;
    }
    if (req._args_count != 1) {
        return 2;
    }
    long index = -1;
    auto& arg = req._args[0];
    if (!(!arg.empty() && std::find_if(arg.begin(), arg.end(), [] (auto c) { return !std::isdigit((char)c); }) == arg.end())) {
        return 1;
    }

    try {
        index = std::atol(sstring{reinterpret_cast<const char*>(arg.data()), arg.size()}.data());
    } catch (std::exception const & e) {
        return 1;
    }
    if (index >= 0 && index < 16) {
        _client_state.set_raw_keyspace(sprint("redis_%d", static_cast<int>(index)));
        return 0;
    }
    return 1;
}

redis_server::connection::connection(redis_server& server, ipv4_addr server_addr, connected_socket&& fd, socket_address addr)
    : _server(server)
    , _server_addr(server_addr)
    , _fd(std::move(fd))
    , _read_buf(_fd.input())
    , _write_buf(_fd.output())
    , _parser(redis::make_ragel_protocol_parser())
    , _client_state(service::client_state::external_redis_tag{}, server._auth_service, addr, "redis_0")
{
    ++_server._total_connections;
    ++_server._current_connections;
    _server._connections_list.push_back(*this);
}

redis_server::connection::~connection() {
    --_server._current_connections;
    _server._connections_list.erase(_server._connections_list.iterator_to(*this));
    _server.maybe_idle();
}

future<> redis_server::connection::process()
{
    return do_until([this] {
        return _read_buf.eof();
    }, [this] {
        return with_gate(_pending_requests_gate, [this] {
            return process_request();
        });
    }).then_wrapped([this] (future<> f) {
        try {
            f.get();
        } /*catch (const exceptions::redis_exception& ex) {
            //write_response(make_error(0, ex.code(), ex.what(), tracing::trace_state_ptr()));
        } catch (std::exception& ex) {
            //write_response(make_error(0, exceptions::exception_code::SERVER_ERROR, ex.what(), tracing::trace_state_ptr()));
        } */ catch (...) {
            //write_response(make_error(0, exceptions::exception_code::SERVER_ERROR, "unknown error", tracing::trace_state_ptr()));
        }
    }).finally([this] {
        return _pending_requests_gate.close().then([this] {
            //_server._notifier->unregister_connection(this);
            return _ready_to_respond.finally([this] {
                return _write_buf.close();
            });
        });
    });
}

future<> redis_server::connection::shutdown()
{
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
    }
    return make_ready_future<>();
}

thread_local redis_server::connection::execution_stage_type redis_server::connection::_process_request_stage{"redis_transport", &connection::process_request_one};

future<> redis_server::connection::process_request() {
    _parser.init();
    return _read_buf.consume(_parser).then([this] {
        ++_server._requests_served;
        ++_server._requests_serving;
        _pending_requests_gate.enter();
        auto leave = defer([this] { _pending_requests_gate.leave(); });
        tracing_request_type tracing_requested = tracing_request_type::not_requested;
        [&] {
            auto cpu = pick_request_cpu();
            // If the SELECT command coming,  Maybe we should change the
            // keyspace of current connection.
            // So do not submit the SELECT command to other shard.
            auto changed = maybe_change_keyspace(_parser.get_request(), tracing_requested);
            if (changed < 0) {
                if (cpu == engine().cpu_id()) {
                    return _process_request_stage(this, std::move(_parser.get_request()), service::client_state(service::client_state::request_copy_tag{}, _client_state, _client_state.get_timestamp()), tracing_requested);
                } else {
                    return smp::submit_to(cpu, [this, request = std::move(_parser.get_request()), client_state = _client_state, tracing_requested, ts = _client_state.get_timestamp()] () mutable {
                        return _process_request_stage(this, request, service::client_state(service::client_state::request_copy_tag{}, client_state, ts), tracing_requested);
                    });
                }
            } else if (changed == 0) {
                // Move these codes to query processor.
                return redis::redis_message::ok().then([] (auto&& message) {
                    return make_ready_future<redis_server::connection::result>(std::move(message));
                });
            } else if (changed == 1) {
                return redis::redis_message::make_exception("-invalid DB index\r\n").then([] (auto&& message) {
                    return make_ready_future<redis_server::connection::result>(std::move(message));
                });
            } else {
                return redis::redis_message::make_exception("-wrong number of arguments for 'select' command\r\n").then([] (auto&& message) {
                    return make_ready_future<redis_server::connection::result>(std::move(message));
                });
            } 
        } ().then_wrapped([this, leave = std::move(leave)] (future<result> result_future) {
            try {
                auto result = result_future.get0();
                --_server._requests_serving;
                auto message = result.make_message();
                _write_buf.write(std::move(*message)).then([this] {
                    _write_buf.flush();
                });
            } catch (...) {
                logging.error("request processing failed: {}", std::current_exception());
            }
        });
        return make_ready_future<>();
    });
}

static inline bytes_view to_bytes_view(temporary_buffer<char>& b)
{
    using byte = bytes_view::value_type;
    return bytes_view(reinterpret_cast<const byte*>(b.get()), b.size());
}


unsigned redis_server::connection::pick_request_cpu()
{
    if (_server._lb == redis_load_balance::round_robin) {
        return _request_cpu++ % smp::count;
    }
    return engine().cpu_id();
}

/*
std::unique_ptr<redis_server::response> redis_server::connection::make_unavailable_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t required, int32_t alive, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(required);
    response->write_int(alive);
    return response;
}

std::unique_ptr<redis_server::response> redis_server::connection::make_read_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_byte(data_present);
    return response;
}

std::unique_ptr<redis_server::response> redis_server::connection::make_read_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state)
{
    if (_version < 4) {
        return make_read_timeout_error(stream, err, std::move(msg), cl, received, blockfor, data_present, tr_state);
    }
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_int(numfailures);
    response->write_byte(data_present);
    return response;
}

std::unique_ptr<redis_server::response> redis_server::connection::make_mutation_write_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_string(sprint("%s", type));
    return response;
}

std::unique_ptr<redis_server::response> redis_server::connection::make_mutation_write_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state)
{
    if (_version < 4) {
        return make_mutation_write_timeout_error(stream, err, std::move(msg), cl, received, blockfor, type, tr_state);
    }
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_int(numfailures);
    response->write_string(sprint("%s", type));
    return response;
}

std::unique_ptr<redis_server::response> redis_server::connection::make_already_exists_error(int16_t stream, exceptions::exception_code err, sstring msg, sstring ks_name, sstring cf_name, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_string(ks_name);
    response->write_string(cf_name);
    return response;
}

std::unique_ptr<redis_server::response> redis_server::connection::make_unprepared_error(int16_t stream, exceptions::exception_code err, sstring msg, bytes id, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_short_bytes(id);
    return response;
}

std::unique_ptr<redis_server::response> redis_server::connection::make_error(int16_t stream, exceptions::exception_code err, sstring msg, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    return response;
}

std::unique_ptr<redis_server::response> redis_server::connection::make_ready(int16_t stream, const tracing::trace_state_ptr& tr_state)
{
    return std::make_unique<redis_server::response>(stream, cql_binary_opcode::READY, tr_state);
}

std::unique_ptr<redis_server::response> redis_server::connection::make_autheticate(int16_t stream, const sstring& clz, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::AUTHENTICATE, tr_state);
    response->write_string(clz);
    return response;
}

std::unique_ptr<redis_server::response> redis_server::connection::make_auth_success(int16_t stream, bytes b, const tracing::trace_state_ptr& tr_state) {
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::AUTH_SUCCESS, tr_state);
    response->write_bytes(std::move(b));
    return response;
}

std::unique_ptr<redis_server::response> redis_server::connection::make_auth_challenge(int16_t stream, bytes b, const tracing::trace_state_ptr& tr_state) {
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::AUTH_CHALLENGE, tr_state);
    response->write_bytes(std::move(b));
    return response;
}

std::unique_ptr<redis_server::response> redis_server::connection::make_supported(int16_t stream, const tracing::trace_state_ptr& tr_state)
{
    std::multimap<sstring, sstring> opts;
    opts.insert({"CQL_VERSION", cql3::query_processor::CQL_VERSION});
    opts.insert({"COMPRESSION", "lz4"});
    opts.insert({"COMPRESSION", "snappy"});
    auto& part = dht::global_partitioner();
    opts.insert({"SCYLLA_SHARD", sprint("%d", engine().cpu_id())});
    opts.insert({"SCYLLA_NR_SHARDS", sprint("%d", smp::count)});
    opts.insert({"SCYLLA_SHARDING_ALGORITHM", part.cpu_sharding_algorithm_name()});
    opts.insert({"SCYLLA_SHARDING_IGNORE_MSB", sprint("%d", part.sharding_ignore_msb())});
    opts.insert({"SCYLLA_PARTITIONER", part.name()});
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::SUPPORTED, tr_state);
    response->write_string_multimap(opts);
    return response;
}

std::unique_ptr<redis_server::response>
redis_server::connection::make_result(int16_t stream, shared_ptr<messages::result_message> msg, const tracing::trace_state_ptr& tr_state, bool skip_metadata)
{
    auto response = std::make_unique<redis_server::response>(stream, cql_binary_opcode::RESULT, tr_state);
    fmt_visitor fmt{_version, *response, skip_metadata};
    msg->accept(fmt);
    return response;
}

void redis_server::connection::write_response(foreign_ptr<std::unique_ptr<redis_server::response>>&& response, cql_compression compression)
{
    _ready_to_respond = _ready_to_respond.then([this, compression, response = std::move(response)] () mutable {
        auto message = response->make_message(_version, compression);
        message.on_delete([response = std::move(response)] { });
        return _write_buf.write(std::move(message)).then([this] {
            return _write_buf.flush();
        });
    });
}

scattered_message<char> redis_server::response::make_message(uint8_t version, cql_compression compression) {
    if (compression != cql_compression::none) {
        compress(compression);
    }
    scattered_message<char> msg;
    auto frame = make_frame(version, _body.size());
    msg.append(std::move(frame));
    for (auto&& fragment : _body.fragments()) {
        msg.append_static(reinterpret_cast<const char*>(fragment.data()), fragment.size());
    }
    return msg;
}
*/
}
