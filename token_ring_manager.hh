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
#pragma once
#include "core/sharded.hh"
#include "core/sstring.hh"
#include <experimental/optional>

struct token {
   size_t _hash;
};

class token_ring_manager;
extern distributed<token_ring_manager> _ring;
inline distributed<token_ring_manager>& ring() {
    return _ring;
}
class token_ring_manager final {
public:
    token_ring_manager() {}
    ~token_ring_manager() {}

    token_ring_manager(const token_ring_manager&) = delete;
    token_ring_manager& operator = (const token_ring_manager&) = delete;
    token_ring_manager(token_ring_manager&&) = delete;
    token_ring_manager& operator = (token_ring_manager&&) = delete;

    const std::vector<inet_address>& get_target_endpoints_for_key(const redis_key& rk) const;
    bool should_served_by_me(const redis_key& rk) const;
    const std::vector<inet_address>& get_replica_nodes_for_write(const redis_key& rk) const;
    const inet_address& get_replica_node_for_read(const redis_key& rk) const;
    const size_t get_replica_count() const { return _replica_count; }
    void set_sorted_tokens(const std::vector<token>& tokens, const std::unordered_map<token, inet_address>& token_to_endpoint);

    future<> stop() {}
private:
    size_t _replica_count = 1;
    size_t _vnode_count = 1023;
    std::vector<token> _sorted_tokens {};
    std::unordered_map<token, inet_address> _token_to_endpoint {};

    // FIXME: need a timer to evict elements by LRU?
    std::unordered_map<token, std::vector<inet_address>> _token_write_targets_endpoints_cache {};
    std::unordered_map<token, inet_address>              _token_read_targets_endpoints_cache {};

    const std::vector<inet_address>& get_replica_nodes_internal(const redis_key& rk) const;
    size_t token_to_index(const token& t) const;
};

