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
*  Copyright (c) 2016-2026, Peng Jian, pstack@163.com. All rights reserved.
*
*/
#include "redis.hh"
#include <iomanip>
#include <sstream>
#include <algorithm>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include "util/log.hh"
#include <unistd.h>
#include <cstdlib>
#include "redis_protocol.hh"
#include "db.hh"
#include "reply_builder.hh"
#include  <experimental/vector>
#include "core/metrics.hh"
using namespace net;
namespace redis {


using logger =  seastar::logger;
static logger redis_log ("redis");


distributed<storage_service> _the_service;

void storage_service::init_messaging_service()
{
}

future<> storage_service::initialize()
{
    int delay = 30;
    return seastar::async([this, delay] {
        get_storage_service().invoke_on_all([] (auto& ss) {
            ss.init_messaging_service();
        }).get();
        auto& gossiper = gms::get_local_gossiper();
        _initialized = true;

        std::vector<inet_address> loaded_endpoints;
        slogger.info("Loading persisted ring state");
        auto loaded_tokens = store::system_meta::load_tokens().get0();
        auto loaded_host_ids = store::system_meta::load_host_ids().get0();

        for (auto& x : loaded_tokens) {
            slogger.debug("Loaded tokens: endpoint={}, tokens={}", x.first, x.second);
        }

        for (auto& x : loaded_host_ids) {
            slogger.debug("Loaded host_id: endpoint={}, uuid={}", x.first, x.second);
        }

        for (auto x : loaded_tokens) {
            auto ep = x.first;
            auto tokens = x.second;
            if (ep == get_broadcast_address()) {
                // entry has been mistakenly added, delete it
                store::system_meta::remove_endpoint(ep).get();
            } else {
                _ring.update_normal_tokens(tokens, ep);
                if (loaded_host_ids.count(ep)) {
                    _ring.update_host_id(loaded_host_ids.at(ep), ep);
                }
                loaded_endpoints.push_back(ep);
                gossiper.add_saved_endpoint(ep);
            }
        }

        prepare_to_join(std::move(loaded_endpoints));

        join_token_ring(delay);
        auto tokens = config::get_saved_tokens().get();
        if (!tokens.empty()) {
            _ring.update_normal_tokens(tokens, get_broadcast_address());
            gossiper.add_local_application_state(gms::application_state::TOKENS, value_factory.tokens(tokens)).get();
            gossiper.add_local_application_state(gms::application_state::STATUS, value_factory.hibernate(true)).get();
        }
        return make_ready_future<>();
    });
}

void storage_service::prepare_to_join(std::vector<inet_address> loaded_endpoints) {
    if (_joined) {
        return;
    }

    auto& conf = db.get_config();
    auto& gossiper = gms::get_local_gossiper();
    auto seeds = gms::get_local_gossiper().get_seeds();
    auto my_ep = get_broadcast_address();
    if (seeds.count(my_ep)) {
        // This node is a seed node
        // This is a existing seed node
        if (seeds.size() == 1) {
            // This node is the only seed node, check features with system table
        } else {
            // More than one seed node in the seed list, do shadow round with other seed nodes
            bool ok;
            try {
                slogger.info("Checking remote features with gossip");
                gossiper.do_shadow_round().get();
                ok = true;
            } catch (...) {
                gossiper.finish_shadow_round();
                ok = false;
            }

            if (ok) {
                gossiper.reset_endpoint_state_map();
                for (auto ep : loaded_endpoints) {
                    gossiper.add_saved_endpoint(ep);
                }
            }
        }
    } else {
        // This node is a non-seed node
        // Do shadow round to check if this node knows all the features
        // advertised by all other nodes, otherwise this node is too old
        // (missing features) to join the cluser.
        slogger.info("Checking remote features with gossip");
        gossiper.do_shadow_round().get();
        gossiper.reset_endpoint_state_map();
        for (auto ep : loaded_endpoints) {
            gossiper.add_saved_endpoint(ep);
        }
    }

    // have to start the gossip service before we can see any info on other nodes.  this is necessary
    // for bootstrap to get the load info it needs.
    // (we won't be part of the storage ring though until we add a counterId to our state, below.)
    // Seed the host ID-to-endpoint map with our own ID.
    auto local_host_id = config::get_local_host_id().get0();
    get_storage_service().invoke_on_all([local_host_id] (auto& ss) {
            ss._local_host_id = local_host_id;
    }).get();
    _ring.update_host_id(local_host_id, get_broadcast_address());
    auto broadcast_rpc_address = utils::fb_utilities::get_broadcast_rpc_address();
    app_states.emplace(gms::application_state::NET_VERSION, value_factory.network_version());
    app_states.emplace(gms::application_state::HOST_ID, value_factory.host_id(local_host_id));
    app_states.emplace(gms::application_state::RPC_ADDRESS, value_factory.rpcaddress(broadcast_rpc_address));
    app_states.emplace(gms::application_state::RELEASE_VERSION, value_factory.release_version());
    slogger.info("Starting up server gossip");

    auto& gossiper = gms::get_local_gossiper();
    gossiper.register_(this->shared_from_this());
    auto generation_number = config::increment_and_get_generation().get0();
    gossiper.start_gossiping(generation_number, app_states).get();
}

void storage_service::join_token_ring(int delay) {
    get_storage_service().invoke_on_all([] (auto&& ss) {
        ss._joined = true;
    }).get();
    // We bootstrap if we haven't successfully bootstrapped before, as long as we are not a seed.
    // If we are a seed, or if the user manually sets auto_bootstrap to false,
    // we'll skip streaming data from other nodes and jump directly into the ring.
    //
    // The seed check allows us to skip the RING_DELAY sleep for the single-node cluster case,
    // which is useful for both new users and testing.
    //
    // We attempted to replace this with a schema-presence check, but you need a meaningful sleep
    // to get schema info from gossip which defeats the purpose.  See CASSANDRA-4427 for the gory details.
    auto& conf = _db.get_config();
    std::unordered_set<inet_address> current;
    slogger.debug("Bootstrap variables: {} {} {} {}",
                 is_auto_bootstrap(),
                 store::system_meta::bootstrap_in_progress(),
                 store::system_meta::bootstrap_complete(),
                 get_seeds().count(get_broadcast_address()));
    if (conf.auto_bootstrap() && !store::system_meta::bootstrap_complete() && get_seeds().count(get_broadcast_address())) {
        slogger.info("This node will not auto bootstrap because it is configured to be a seed node.");
    }
    if (should_bootstrap()) {
        if (store::system_meta::bootstrap_in_progress()) {
            slogger.warn("Detected previous bootstrap failure; retrying");
        } else {
            store::system_meta::set_bootstrap_state(store::system_meta::bootstrap_state::IN_PROGRESS).get();
        }
        set_mode(mode::JOINING, "waiting for ring information", true);
        // first sleep the delay to make sure we see all our peers
        for (int i = 0; i < delay; i += 1000) {
            // we will recieve some meta info from remote peers
            //sleep(std::chrono::seconds(1)).get();
        }
        set_mode(mode::JOINING, "all complete, ready to bootstrap", true);

        auto t = gms::gossiper::clk::now();
        while (get_property_rangemovement() &&
            (!_ring.get_bootstrap_tokens().empty() ||
             !_ring.get_leaving_endpoints().empty() ||
             !_ring.get_moving_endpoints().empty())) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(gms::gossiper::clk::now() - t).count();
            slogger.info("Checking bootstrapping/leaving/moving nodes: tokens {}, leaving {}, moving {}, sleep 1 second and check again ({} seconds elapsed)",
                _ring.get_bootstrap_tokens().size(),
                _ring.get_leaving_endpoints().size(),
                _ring.get_moving_endpoints().size(),
                elapsed);

            sleep(std::chrono::seconds(1)).get();

            if (gms::gossiper::clk::now() > t + std::chrono::seconds(60)) {
                throw std::runtime_error("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while consistent_rangemovement is true");
            }

        }
        slogger.info("Checking bootstrapping/leaving/moving nodes: ok");

        if (!db().local().is_replacing()) {
            if (_ring.is_member(get_broadcast_address())) {
                throw std::runtime_error("This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)");
            }
            set_mode(mode::JOINING, "getting bootstrap token", true);
            _bootstrap_tokens = boot_strapper::get_bootstrap_tokens(_ring, _db.local());
        } else {
            auto replace_addr = db().local().get_replace_address();
            if (replace_addr && *replace_addr != get_broadcast_address()) {
                // Sleep additionally to make sure that the server actually is not alive
                // and giving it more time to gossip if alive.
                sleep(service::load_broadcaster::BROADCAST_INTERVAL).get();

                // check for operator errors...
                for (auto token : _bootstrap_tokens) {
                    auto existing = _token_metadata.get_endpoint(token);
                    if (existing) {
                        auto& gossiper = gms::get_local_gossiper();
                        auto eps = gossiper.get_endpoint_state_for_endpoint(*existing);
                        if (eps && eps->get_update_timestamp() > gms::gossiper::clk::now() - std::chrono::milliseconds(delay)) {
                            throw std::runtime_error("Cannot replace a live node...");
                        }
                        current.insert(*existing);
                    } else {
                        throw std::runtime_error(sprint("Cannot replace token %s which does not exist!", token));
                    }
                }
            } else {
                sleep(get_ring_delay()).get();
            }
            std::stringstream ss;
            ss << _bootstrap_tokens;
            set_mode(mode::JOINING, sprint("Replacing a node with token(s): %s", ss.str()), true);
        }
        bootstrap(_bootstrap_tokens);
        // bootstrap will block until finished
        if (_is_bootstrap_mode) {
            auto err = sprint("We are not supposed in bootstrap mode any more");
            slogger.warn(err.c_str());
            throw std::runtime_error(err);
        }
    } else {
        size_t num_tokens = conf.num_tokens();
        _bootstrap_tokens = store::system_meta::get_saved_tokens().get0();
        if (_bootstrap_tokens.empty()) {
            auto initial_tokens = get_initial_tokens();
            if (initial_tokens.size() < 1) {
                _bootstrap_tokens = boot_strapper::get_random_tokens(_token_metadata, num_tokens);
                slogger.info("Generated random tokens. tokens are {}", _bootstrap_tokens);
            } else {
                for (auto token_string : initial_tokens) {
                    auto token = redis::default_partitioner().from_sstring(token_string);
                    _bootstrap_tokens.insert(token);
                }
                slogger.info("Saved tokens not found. Using configuration value: {}", _bootstrap_tokens);
            }
        } else {
            if (_bootstrap_tokens.size() != num_tokens) {
                throw std::runtime_error(sprint("Cannot change the number of tokens from %ld to %ld", _bootstrap_tokens.size(), num_tokens));
            } else {
                slogger.info("Using saved tokens {}", _bootstrap_tokens);
            }
        }
    }

    store::system_meta::set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED).get();
    set_tokens(_bootstrap_tokens);
    // remove the existing info about the replaced node.
    if (!current.empty()) {
        auto& gossiper = gms::get_local_gossiper();
        for (auto existing : current) {
            gossiper.replaced_endpoint(existing);
        }
    }
    if (_ring.sorted_tokens().empty()) {
        auto err = sprint("join_token_ring: Sorted token in token_metadata is empty");
        slogger.error(err.c_str());
        throw std::runtime_error(err);
    }
    slogger.info("Startup complete!");
}

future<> storage_service::stop()
{
    return make_ready_future<>();
}
}
