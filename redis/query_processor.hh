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

/*
 * Modified by pengjian.uestc at gmail.com
 */

#pragma once

#include <experimental/string_view>
#include <unordered_map>

#include <seastar/core/distributed.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/shared_ptr.hh>

#include "exceptions/exceptions.hh"
#include "log.hh"
#include "service/migration_manager.hh"
#include "service/query_state.hh"
#include "transport/messages/result_message.hh"

class timeout_config;

namespace redis {

struct request;
struct reply;
class redis_message;
class query_processor {
    service::storage_proxy& _proxy;
    distributed<database>& _db;
    seastar::metrics::metric_groups _metrics;
public:
    query_processor(service::storage_proxy& proxy, distributed<database>& db);

    ~query_processor();

    distributed<database>& db() {
        return _db;
    }

    service::storage_proxy& proxy() {
        return _proxy;
    }

    future<redis_message> process(request&&, service::client_state&, const timeout_config& config);

    future<> stop();
};

extern distributed<query_processor> _the_query_processor;

inline distributed<query_processor>& get_query_processor() {
    return _the_query_processor;
}

inline query_processor& get_local_query_processor() {
    return _the_query_processor.local();
}

}
