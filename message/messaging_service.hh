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

#include "messaging_service_fwd.hh"
#include "core/reactor.hh"
#include "core/distributed.hh"
#include "core/sstring.hh"
#include "gms/inet_address.hh"
#include "rpc/rpc_types.hh"
#include <unordered_map>
#include "range.hh"
#include "digest_algorithm.hh"

#include <seastar/net/tls.hh>

// forward declarations
namespace streaming {
    class prepare_message;
}

namespace gms {
    class gossip_digest_syn;
    class gossip_digest_ack;
    class gossip_digest_ack2;
}

namespace redis {
    class internal_read_message;
    class internal_write_message;
}

namespace utils {
    class UUID;
}

namespace net {

/* All verb handler identifiers */
enum class messaging_verb : int32_t {
    CLIENT_ID          = 0,
    // Used by gossip
    GOSSIP_DIGEST_SYN  = 1,
    GOSSIP_DIGEST_ACK  = 2,
    GOSSIP_DIGEST_ACK2 = 3,
    GOSSIP_ECHO        = 4,
    GOSSIP_SHUTDOWN    = 5,
    // end of gossip verb
    // Used by internal request
    INTERNAL_READ      = 6,
    INTERNAL_WRITE     = 7,
    LAST               = 8,
};

} // namespace net

namespace std {
template <>
class hash<net::messaging_verb> {
public:
    size_t operator()(const net::messaging_verb& x) const {
        return hash<int32_t>()(int32_t(x));
    }
};
} // namespace std

namespace net {

struct serializer {};

struct msg_addr {
    gms::inet_address addr;
    uint32_t cpu_id;
    friend bool operator==(const msg_addr& x, const msg_addr& y);
    friend bool operator<(const msg_addr& x, const msg_addr& y);
    friend std::ostream& operator<<(std::ostream& os, const msg_addr& x);
    struct hash {
        size_t operator()(const msg_addr& id) const;
    };
};

class messaging_service : public seastar::async_sharded_service<messaging_service> {
public:
    struct rpc_protocol_wrapper;
    struct rpc_protocol_client_wrapper;
    struct rpc_protocol_server_wrapper;
    struct shard_info;

    using msg_addr = net::msg_addr;
    using inet_address = gms::inet_address;
    using UUID = utils::UUID;
    using clients_map = std::unordered_map<msg_addr, shard_info, msg_addr::hash>;

    // This should change only if serialization format changes
    static constexpr int32_t current_version = 0;

    struct shard_info {
        shard_info(shared_ptr<rpc_protocol_client_wrapper>&& client);
        shared_ptr<rpc_protocol_client_wrapper> rpc_client;
        rpc::stats get_stats() const;
    };

    void foreach_client(std::function<void(const msg_addr& id, const shard_info& info)> f) const;

    void increment_dropped_messages(messaging_verb verb);

    uint64_t get_dropped_messages(messaging_verb verb) const;

    const uint64_t* get_dropped_messages() const;

    int32_t get_raw_version(const gms::inet_address& endpoint) const;

    bool knows_version(const gms::inet_address& endpoint) const;

    enum class encrypt_what {
        none,
        rack,
        dc,
        all,
    };

    enum class compress_what {
        none,
        dc,
        all,
    };

private:
    gms::inet_address _listen_address;
    uint16_t _port;
    uint16_t _ssl_port;
    encrypt_what _encrypt_what;
    compress_what _compress_what;
    bool _should_listen_to_broadcast_address;
    // map: Node broadcast address -> Node internal IP for communication within the same data center
    std::unordered_map<gms::inet_address, gms::inet_address> _preferred_ip_cache;
    std::unique_ptr<rpc_protocol_wrapper> _rpc;
    std::array<std::unique_ptr<rpc_protocol_server_wrapper>, 2> _server;
    ::shared_ptr<seastar::tls::server_credentials> _credentials;
    std::array<std::unique_ptr<rpc_protocol_server_wrapper>, 2> _server_tls;
    std::array<clients_map, 4> _clients;
    uint64_t _dropped_messages[static_cast<int32_t>(messaging_verb::LAST)] = {};
    bool _stopping = false;
public:
    using clock_type = lowres_clock;
public:
    messaging_service(gms::inet_address ip = gms::inet_address("0.0.0.0"),
            uint16_t port = 7000, bool listen_now = true);
    messaging_service(gms::inet_address ip, uint16_t port, encrypt_what, compress_what,
            uint16_t ssl_port, std::shared_ptr<seastar::tls::credentials_builder>,
            bool sltba = false, bool listen_now = true);
    ~messaging_service();
public:
    void start_listen();
    uint16_t port();
    gms::inet_address listen_address();
    future<> stop_tls_server();
    future<> stop_nontls_server();
    future<> stop_client();
    future<> stop();
    static rpc::no_wait_type no_wait();
    bool is_stopping() { return _stopping; }
public:
    gms::inet_address get_preferred_ip(gms::inet_address ep);
    future<> init_local_preferred_ip_cache();
    void cache_preferred_ip(gms::inet_address ep, gms::inet_address ip);

    // Wrapper for GOSSIP_ECHO verb
    void register_gossip_echo(std::function<future<> ()>&& func);
    void unregister_gossip_echo();
    future<> send_gossip_echo(msg_addr id);

    // Wrapper for GOSSIP_SHUTDOWN
    void register_gossip_shutdown(std::function<rpc::no_wait_type (inet_address from)>&& func);
    void unregister_gossip_shutdown();
    future<> send_gossip_shutdown(msg_addr id, inet_address from);

    // Wrapper for GOSSIP_DIGEST_SYN
    void register_gossip_digest_syn(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, gms::gossip_digest_syn)>&& func);
    void unregister_gossip_digest_syn();
    future<> send_gossip_digest_syn(msg_addr id, gms::gossip_digest_syn msg);

    // Wrapper for GOSSIP_DIGEST_ACK
    void register_gossip_digest_ack(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, gms::gossip_digest_ack)>&& func);
    void unregister_gossip_digest_ack();
    future<> send_gossip_digest_ack(msg_addr id, gms::gossip_digest_ack msg);

    // Wrapper for GOSSIP_DIGEST_ACK2
    void register_gossip_digest_ack2(std::function<rpc::no_wait_type (gms::gossip_digest_ack2)>&& func);
    void unregister_gossip_digest_ack2();
    future<> send_gossip_digest_ack2(msg_addr id, gms::gossip_digest_ack2 msg);

    // Wrapper for INTERNAL_DATA_BUS_REQUEST
    /*
    void register_internal_read(std::function<future<foreign_ptr<lw_shared_ptr<scattered_message<char>>> (const rpc::client& cinfo, internal_read_request)>&& fun);
    void unregister_internal_read();
    future<foreign_ptr<lw_shared_ptr<scattered_message<char>>> send_internal_read(msg_addr id, internal_read_request msg);


    void register_internal_write(std::function<future<foreign_ptr<lw_shared_ptr<scattered_message<char>>> (const rpc::client& cinfo, internal_write_message)>&& fun);
    void unregister_internal_write();
    future<foreign_ptr<lw_shared_ptr<scattered_message<char>>> send_internal_write(msg_addr id, internal_write_message msg);
    */
    void foreach_server_connection_stats(std::function<void(const rpc::client_info&, const rpc::stats&)>&& f) const;
public:
    // Return rpc::protocol::client for a shard which is a ip + cpuid pair.
    shared_ptr<rpc_protocol_client_wrapper> get_rpc_client(messaging_verb verb, msg_addr id);
    void remove_rpc_client_one(clients_map& clients, msg_addr id, bool dead_only);
    void remove_error_rpc_client(messaging_verb verb, msg_addr id);
    void remove_rpc_client(msg_addr id);
    std::unique_ptr<rpc_protocol_wrapper>& rpc();
    static msg_addr get_source(const rpc::client_info& client);
};

extern distributed<messaging_service> _the_messaging_service;

inline distributed<messaging_service>& get_messaging_service() {
    return _the_messaging_service;
}

inline messaging_service& get_local_messaging_service() {
    return _the_messaging_service.local();
}

} // namespace net
