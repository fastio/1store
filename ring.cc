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
#include "ring.hh"
#include "db.hh"
#include "keys.hh"
namespace redis {

future<> ring::start()
{
    return make_ready_future<>();
}

future<> ring::stop()
{
    return make_ready_future<>();
}

size_t ring::token_to_index(const token& t) const
{
    assert(_sorted_tokens.empty() == false);
    auto it = std::lower_bound(_sorted_tokens.begin(), _sorted_tokens.end(), t);
    if (it == _sorted_tokens.end()) {
        return 0;
    }
    return std::distance(_sorted_tokens.begin(), it);
}

const std::vector<gms::inet_address> ring::get_replica_nodes_internal(const redis_key& rk)
{
    auto first_token_index = token_to_index(token{ token::kind::key, {rk.data(), rk.size()} });
    auto& first_token = _sorted_tokens[first_token_index];
    auto targets = _token_write_targets_endpoints_cache.find(first_token);
    if (targets != _token_write_targets_endpoints_cache.end()) {
        return targets->second;
    }
    std::vector<gms::inet_address> new_targets;
    new_targets.reserve(_replica_count);

    new_targets.emplace_back(_token_to_endpoint[first_token]);
    auto all_tokens = _sorted_tokens.size();
    for (size_t i = 1; i < _replica_count; ++i) {
        new_targets.emplace_back(_token_to_endpoint[_sorted_tokens[(i + first_token_index) % all_tokens]]);
    }
    // cache the result
    _token_write_targets_endpoints_cache[first_token] = new_targets;
    return new_targets;
}

const std::vector<gms::inet_address> ring::get_replica_nodes_for_write(const redis_key& rk)
{
    return get_replica_nodes_internal(rk);
}


const gms::inet_address ring::get_replica_node_for_read(const redis_key& rk)
{
    auto first_token_index = token_to_index(token{ token::kind::key, {rk.data(), rk.size()} });
    auto& first_token = _sorted_tokens[first_token_index];
    auto targets = _token_read_targets_endpoints_cache.find(first_token);
    if (targets != _token_read_targets_endpoints_cache.end()) {
        return targets->second;
    }
    auto target = _token_to_endpoint[first_token];
    _token_read_targets_endpoints_cache[first_token] = target;
    return target;
}

void ring::set_sorted_tokens(const std::vector<token>& tokens, const std::unordered_map<token, gms::inet_address>& token_to_endpoint)
{
    _sorted_tokens = tokens;
    _token_to_endpoint = token_to_endpoint;
    _token_write_targets_endpoints_cache.clear();
    _token_read_targets_endpoints_cache.clear();
}

std::chrono::milliseconds ring::get_ring_delay() const{
    auto ring_delay = get_local_database().get_config().ring_delay_ms();
    return std::chrono::milliseconds(ring_delay);
}
}
