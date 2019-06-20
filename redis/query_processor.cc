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

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1

#include "redis/query_processor.hh"
#include "redis/command_factory.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "redis/abstract_command.hh"
#include <seastar/core/metrics.hh>
#include "timeout_config.hh"
#include "log.hh"
namespace redis {

static logging::logger logging("redisqp");
distributed<query_processor> _the_query_processor;

query_processor::query_processor(service::storage_proxy& proxy, distributed<database>& db)
        : _proxy(proxy)
        , _db(db)
{
    namespace sm = seastar::metrics;
}

query_processor::~query_processor() {
}

future<> query_processor::stop() {
    return make_ready_future<>();
}

future<redis_message> query_processor::process(request&& req, service::client_state& client_state, const timeout_config& config) {
    // FIXME: timeout, consistency level should be configurable.
    return do_with(command_factory::create(_proxy, client_state, std::move(req)), [this, &client_state, config] (auto& e) {
        return e->execute(_proxy, db::consistency_level::LOCAL_ONE, db::timeout_clock::now(), config, client_state);
    });
}

}
