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
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/shared_ptr.hh"
#include "core/sharded.hh"
#include "core/sstring.hh"
#include "util/log.hh"
#include "common.hh"
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include "utils/managed_bytes.hh"
#include "utils/managed_ref.hh"
#include "utils/bytes.hh"
#include "utils/allocation_strategy.hh"
#include "utils/logalloc.hh"
#include  <experimental/vector>
#include <experimental/optional>
#include  <vector>

using logger =  seastar::logger;
static logger sset_log ("sset");

namespace redis {
struct sset_entry
{
    using set_hook_type = boost::intrusive::set_member_hook<>;
    using list_hook_type = boost::intrusive::list_member_hook<>;
    list_hook_type _list_link;
    set_hook_type _set_link;
    managed_bytes _key;
    size_t _key_hash;
    double _score;

    sset_entry(const sstring& key, const double score) noexcept
        : _list_link()
        , _set_link()
        , _key(std::move(*(current_allocator().construct<managed_bytes>(bytes_view {reinterpret_cast<const signed char*>(key.data()), key.size()}))))
        , _key_hash(std::hash<managed_bytes>()(_key))
        , _score(score)
    {
    }

    sset_entry(sset_entry&& o) noexcept
        : _list_link(std::move(o._list_link))
        , _set_link(std::move(o._set_link))
        , _key(std::move(o._key))
        , _key_hash(std::move(o._key_hash))
        , _score(std::move(o._score))
    {
    }

    struct compare {
        inline bool compare_impl(const char* d1, size_t s1, const char* d2, size_t s2) const noexcept {
            const int len = std::min(s1, s2);
            auto r = memcmp(d1, d2, len);
            if (r == 0) {
                if (s1 < s2) return true;
                else if (s1 > s2) return false;
            }
            return r < 0;
        }
        inline bool operator () (const sset_entry& l, const sset_entry& r) const noexcept {
            return compare_impl(l.key_data(), l.key_size(), r.key_data(), r.key_size());
        }
        inline bool operator () (const sstring& k, const sset_entry& e) const noexcept {
            return compare_impl(k.data(), k.size(), e.key_data(), e.key_size());
        }
        inline bool operator () (const sset_entry& e, const sstring& k) const noexcept {
            return compare_impl(e.key_data(), e.key_size(), k.data(), k.size());
        }
        inline bool operator () (const double& score, const sset_entry& e) const noexcept {
            return score < e.score();
        }
        inline bool operator () (const sset_entry& e, const double& score) const noexcept {
            return e.score() < score;
        }
    };

    const managed_bytes& key() const {
        return _key;
    }
    inline const char* key_data() const
    {
        return reinterpret_cast<const char*>(_key.data());
    }
    inline size_t key_size() const
    {
        return _key.size();
    }
    inline const double score() const {
        return _score;
    }
    inline double score() {
        return _score;
    }
    inline void update_score(const double nscore) {
        _score = nscore;
    }
};

class database;
class sset_lsa final {
    friend class database;
    using dict_type = boost::intrusive::set<sset_entry,
        boost::intrusive::member_hook<sset_entry, sset_entry::set_hook_type, &sset_entry::_set_link>,
        boost::intrusive::compare<sset_entry::compare>>;
    using list_type = boost::intrusive::list<sset_entry,
        boost::intrusive::member_hook<sset_entry, boost::intrusive::list_member_hook<>,
        &sset_entry::_list_link>>;
    dict_type _dict;
    list_type _list;
public:
    sset_lsa() noexcept : _dict(), _list()
    {
    }
    sset_lsa(sset_lsa&& o) noexcept : _dict(std::move(o._dict)), _list(std::move(o._list))
    {
    }
    ~sset_lsa()
    {
        flush_all();
    }
    void flush_all()
    {
        _list.clear_and_dispose(current_deleter<sset_entry>());
    }

    inline bool insert(sset_entry* e)
    {
        assert(e != nullptr);
        if (insert_ordered(e)) {
            auto r = _dict.insert(*e);
            sset_log.info("insert success, list size {}, dict size {}", _list.size(), _dict.size());
            return r.second;
        }
        sset_log.info("insert failed, list size {}, dict size {}", _list.size(), _dict.size());
        return false;
    }

    size_t insert_if_not_exists(std::unordered_map<sstring, double>& members)
    {
        size_t inserted = 0;
        for (auto& member : members) {
            const auto& key = member.first;
            const auto& score = member.second;
            if (_dict.find(key, sset_entry::compare()) == _dict.end()) {
                auto entry = current_allocator().construct<sset_entry>(key, score);
                if (insert(entry)) {
                    inserted++;
                }
                else {
                    current_allocator().destroy<sset_entry>(entry);
                }
            }
        }
        return inserted;
    }

    size_t update_if_only_exists(std::unordered_map<sstring, double>& members)
    {
        size_t inserted = 0;
        for (auto& member : members) {
            const auto& key = member.first;
            const auto& score = member.second;
            auto it = _dict.find(key, sset_entry::compare());
            if (it != _dict.end()) {
                //remove it from list, update score, insert it to list again.
                auto lit = list_type::s_iterator_to(*it);
                _list.erase(lit);
                it->update_score(score);
                if (insert_ordered(&(*it))) {
                    inserted++;
                }
            }
        }
        return inserted;
    }

    size_t insert_or_update(std::unordered_map<sstring, double>& members)
    {
        size_t inserted = 0;
        for (auto& member : members) {
            const auto& key = member.first;
            const auto& score = member.second;
            auto it = _dict.find(key, sset_entry::compare());
            if (it != _dict.end()) {
                it->update_score(score);
                if (update(&(*it))) {
                    inserted++;
                }
            }
            else {
                auto entry = current_allocator().construct<sset_entry>(key, score);
                if (insert(entry)) {
                    inserted++;
                }
            }
        }
        return inserted;
    }

    void fetch_by_rank(long begin, long end, std::vector<const sset_entry*>& entries) const
    {
        if (_list.empty()) {
            return;
        }
        auto size = _list.size();
        if (begin < 0) { begin += static_cast<long>(size); }
        while (end < 0) { end += static_cast<long>(size); }
        if (begin < 0) begin = 0;
        if (begin > end) {
           return;
        }
        if (begin > static_cast<long>(size)) {
            begin = static_cast<long>(size);
        }
        long rank = 0;
        if (rank_out_of_range(begin, end)) {
            return;
        }
        for (auto it = _list.begin(); it != _list.end(); ++it, ++rank) {
            if (begin <= rank && rank <= end) {
                const auto& e = *it;
                entries.push_back(&e);
            }
        }
    }

    void fetch_by_score(const double min, const double max, std::vector<const sset_entry*>& entries) const
    {
        if (_list.empty()) {
            return;
        }
        if (score_out_of_range(min, max)) {
            return;
        }
        for (auto it = _list.begin(); it != _list.end(); ++it) {
            const auto& score = it->score();
            if (score < min) {
                continue;
            }
            else if (score > max) {
                break;
            }
            else {
                const auto& e = *it;
                entries.push_back(&e);
            }
        }
    }

    bool update_score(const sstring& key, double delta)
    {
        auto it = _dict.find(key, sset_entry::compare());
        if (it != _dict.end()) {
            auto lit = list_type::s_iterator_to(*it);
            _list.erase(lit);
            it->update_score(delta);
            return insert_ordered(&(*it));
        }
        return false;
    }

    size_t erase(std::vector<const sset_entry*>& entries)
    {
        for (size_t i = 0; i < entries.size(); ++i) {
            auto lit = list_type::s_iterator_to(*entries[i]);
            auto dit = dict_type::s_iterator_to(*lit);
            _list.erase(lit);
            _dict.erase_and_dispose(dit, current_deleter<sset_entry>());
        }
        return entries.size();
    }

    size_t erase(std::vector<sstring>& keys)
    {
        size_t removed = 0;
        for (size_t i = 0; i < keys.size(); ++i) {
            auto& key = keys[i];
            auto dit = _dict.find(key, sset_entry::compare());
            if (dit != _dict.end()) {
                auto lit = list_type::s_iterator_to(*dit);
                _list.erase(lit);
                _dict.erase_and_dispose(dit, current_deleter<sset_entry>());
                removed++;
            }
        }
        return removed;
    }

    size_t count_by_score(const double min, const double max) const
    {
        if (_dict.empty()) {
            return 0;
        }
        if (score_out_of_range(min, max)) {
            return 0;
        }
        size_t count = 0;
        for (auto it = _list.begin(); it != _list.end(); ++it) {
            auto score = it->score();
            if (min < score && score < max) {
                count++;
            }
        }
        return count;
    }

    template <typename Func>
    inline std::result_of_t<Func(const sset_entry* e)> with_entry_run(const sstring& k, Func&& func) const {
        auto it = _dict.find(k, sset_entry::compare());
        if (it != _dict.end()) {
            const auto& e = *it;
            return func(&e);
        }
        else {
            return func(nullptr);
        }
    }

    template <typename Func>
    inline std::result_of_t<Func(sset_entry* e)> with_entry_run(const sstring& k, Func&& func) {
        auto it = _dict.find(k, sset_entry::compare());
        if (it != _dict.end()) {
            auto& e = *it;
            return func(&e);
        }
        else {
            return func(nullptr);
        }
    }

    inline size_t size() const
    {
        return _list.size();
    }

    inline bool empty() const
    {
        return _list.empty();
    }

    void erase(const sstring& key)
    {
        auto removed = _dict.erase(_dict.find(key, sset_entry::compare()));
        if (removed != _dict.end()) {
            _list.erase_and_dispose(_list.iterator_to(*removed), current_deleter<sset_entry>());
        }
    }

    std::experimental::optional<size_t> rank(const sstring& key) const
    {
        return  std::experimental::optional<size_t>();
    }
    std::experimental::optional<double> score(const sstring& key) const
    {
        return  std::experimental::optional<double>();
    }
private:
    inline bool score_out_of_range(double min, double max) const
    {
        return !((min > _list.rbegin()->score()) || (max < _list.begin()->score()));
    }

    inline bool rank_out_of_range(long begin, long end) const
    {
        return begin > static_cast<long>(_list.size()) || end <= 0;
    }

    inline bool insert_ordered(sset_entry* e)
    {
        return false;
    }

    inline bool update(sset_entry* e)
    {
        auto lit = list_type::s_iterator_to(*e);
        _list.erase(lit);
        return insert_ordered(e);
    }
};
}
