/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
#include "dict.hh"
#include <functional>
#include "iterator.hh"
namespace redis {
bool item_equal(item_ptr& l, item_ptr& r)
{
    if (!l || !r)
        return false;

    return l->key_size() == r->key_size() &&
           l->key() == r->key();
}

struct dict_node
{
    item* _val;
    struct dict_node *_next;
    dict_node() : _val(nullptr), _next(nullptr) {}
    ~dict_node()
    {
        if (_val) {
            intrusive_ptr_release(_val);
        }
    }
};

struct dict_hash_table
{
    dict_node** _table;
    unsigned long _size;
    unsigned long _size_mask;
    unsigned long _used;
    dict_hash_table() : _table(nullptr), _size(0), _size_mask(0), _used(0) {}
};

struct dict::rep
{
    friend class dict_iterator;
    dict_hash_table _ht[2];
    long _rehash_idx;
    int _iterators;
    static const int dict_can_resize = 1;
    static const unsigned int dict_force_resize_ratio = 5;
    static const int DICT_HT_INITIAL_SIZE = 64;
    std::function<void(item* val)> _free_value_fn;

    rep();
    ~rep();

    int expand_room(unsigned long size);
    int add(const redis_key& key, item* val);
    int replace(const redis_key& key, item* val);
    int remove(const redis_key& key);


    dict_node *add_raw(const redis_key& key);
    dict_node *replace_raw(const redis_key& key);
    int remove_no_free(const redis_key& key);
    void dict_release();
    dict_node * find(const redis_key& key);
    item* fetch_value(const redis_key& key);

    int resize_room();
    dict_node *random_fetch(); 
    unsigned int fetch_some_keys(dict_node **des, unsigned int count);
    int do_rehash(int n);
    unsigned long dict_next_size(unsigned long size);
    void do_rehash_step();
    int generic_delete(const redis_key& key, int nofree);
    int clear(dict_hash_table *ht);
    size_t size();
    std::vector<item_ptr> fetch();
    std::vector<item_ptr> fetch(const std::unordered_set<sstring>& keys);
private:
    static const int DICT_HT_INITAL_SIZE = 4;
    inline bool key_equal(std::experimental::string_view& k, const sstring& key) {
        if (k.size() != key.size()) {
          return false;
        }
        return memcmp(k.data(), key.data(), k.size()) == 0;
    }
    inline bool key_equal(std::experimental::string_view& k, size_t kh, item* val) {
        if (val == nullptr) {
            return false;
        }
        if (kh != val->key_hash()) {
            return false;
        }
        if (k.size() != val->key_size()) {
          return false;
        }
        return memcmp(k.data(), val->key().data(), val->key_size()) == 0;
    }
    inline bool key_equal(const sstring* l, size_t kh, item* val) {
        if (l == nullptr || val == nullptr) {
            return false;
        }
        if (kh != val->key_hash()) {
            return false;
        }
        if (l->size() != val->key_size()) {
          return false;
        }
        return memcmp(l->c_str(), val->key().data(), val->key_size()) == 0;
    }
    inline bool dict_is_rehashing() {
        return _rehash_idx != -1;
    }
    inline int expend_if_needed();
    inline int key_index(const redis_key& key);
    inline void hash_table_reset(dict_hash_table* ht) {
        ht->_table = nullptr;
        ht->_size = 0;
        ht->_size_mask = 0;
        ht->_used = 0;
    }
};

class dict_iterator : public iterator<dict_node>
{
private:
    dict_node* _current;
    dict_node* _next;
    dict_hash_table* _ht;
    int _table_index;
    size_t _index;
    dict::rep* _rep;
    int _status;
public:
    dict_iterator(dict::rep* rep)
        : _current(nullptr)
        , _next(nullptr)
        , _ht(nullptr)
        , _table_index(0)
        , _index(0)
        , _rep(rep)
        , _status(REDIS_ERR)
    {
        _ht = &(_rep->_ht[_table_index]);
        // to prevent the rehash operation
        _rep->_iterators++;
    }
    ~dict_iterator()
    {
        _rep->_iterators--;
    }
    bool valid() const { return _ht != nullptr; }
    void seek_to_first() {
        _table_index = 0;
        _ht = &(_rep->_ht[_table_index]);
        _index = 0;
        do {
            _current = _ht->_table[_index];
            if (_current == nullptr) ++_index;
            if (_index >= _ht->_size) {
                if (_table_index == 0) {
                    ++_table_index;
                    _ht = &(_rep->_ht[_table_index]);
                    _index = 0;
                    continue;
                }
                else {
                    break;
                }
            }
        } while (_current == nullptr);
        if (_current != nullptr) {
            _status = REDIS_OK;
        }
    }
    void seek_to_last() {
    }
    void seek(const sstring& key) {
    }
    void next() {
        for(;;) {
            if (_current == nullptr) {
                _index++;
                if (_index > _ht->_size_mask) {
                    if (_table_index == 0) {
                        _table_index++;
                        _ht = &(_rep->_ht[_table_index]);
                        _index = 0;
                        if (_ht->_size == 0) {
                            // we iteratored all nodes.
                            _status = REDIS_ERR;
                            break;
                        }

                    }
                    else {
                        // we iteratored all nodes.
                        _status = REDIS_ERR;
                        break;
                    }
                }
                _current = _ht->_table[_index];
            }
            else {
                _current = _current->_next;
            }
            if (_current != nullptr) {
                break;
            }
        }
    }
    void prev() {
    }
    dict_node* value() const {
        if (_current != nullptr) {
            return _current;
        }
        return nullptr;
    }
    inline int status() const { return _status; }
};

dict::rep::rep()
   : _rehash_idx(-1)
   , _iterators(0)
   , _free_value_fn([](item* it) { if (it) intrusive_ptr_release(it); })
{
}

int dict::rep::resize_room()
{
    int minimal;

    if (!dict_can_resize || dict_is_rehashing()) return REDIS_ERR;
    minimal = _ht[0]._used;
    if (minimal < DICT_HT_INITIAL_SIZE)
        minimal = DICT_HT_INITIAL_SIZE;
    return expand_room(minimal);
}

int dict::rep::expand_room(unsigned long size)
{
    dict_hash_table n;
    unsigned long realsize = dict_next_size(size);

    if (dict_is_rehashing() || _ht[0]._used > size)
        return REDIS_ERR;

    if (realsize == _ht[0]._size) return REDIS_ERR;

    n._size = realsize;
    n._size_mask = realsize-1;

    n._table = new dict_node*[realsize * sizeof(dict_node*)];
    n._used = 0;
    for (unsigned long i = 0; i < realsize; ++i) n._table[i] = nullptr;
    if (_ht[0]._table == nullptr) {
        _ht[0] = n;
        return REDIS_OK;
    }

    _ht[1] = n;
    _rehash_idx = 0;
    return REDIS_OK;
}

int dict::rep::do_rehash(int n)
{
    int empty_visits = n * 10;
    if (!dict_is_rehashing()) return 0;

    while(n-- && _ht[0]._used != 0) {
        dict_node *de, *nextde;

        assert(_ht[0]._size > (unsigned long)_rehash_idx);
        while(_ht[0]._table[_rehash_idx] == nullptr) {
            _rehash_idx++;
            if (--empty_visits == 0) return 1;
        }
        de = _ht[0]._table[_rehash_idx];
        while(de) {
            unsigned int h;

            nextde = de->_next;
            h = de->_val->key_hash() & _ht[1]._size_mask;
            de->_next = _ht[1]._table[h];
            _ht[1]._table[h] = de;
            _ht[0]._used--;
            _ht[1]._used++;
            de = nextde;
        }
        _ht[0]._table[_rehash_idx] = nullptr;
        _rehash_idx++;
    }

    if (_ht[0]._used == 0) {
        delete [] _ht[0]._table;
        _ht[0] = _ht[1];
        hash_table_reset(&_ht[1]);
        _rehash_idx = -1;
        return 0;
    }

    return 1;
}

size_t dict::rep::size()
{
    return _ht[0]._used + _ht[1]._used;
}

void dict::rep::do_rehash_step()
{
    if (_iterators == 0) do_rehash(1);
}

int dict::rep::add(const redis_key& key, item *val)
{
    dict_node *entry = add_raw(key);
    if (!entry) return REDIS_ERR;
    entry->_val = val;
    return REDIS_OK;
}

dict_node* dict::rep::add_raw(const redis_key& key)
{
    int index;
    dict_node *entry;
    dict_hash_table *ht = nullptr;

    if (dict_is_rehashing()) do_rehash_step();

    if ((index = key_index(key)) == -1)
        return nullptr;

    ht = dict_is_rehashing() ? &_ht[1] : &_ht[0];
    entry = new dict_node();
    entry->_next = ht->_table[index];
    ht->_table[index] = entry;
    ht->_used++;

    return entry;
}

int dict::rep::replace(const redis_key& key, item *val)
{
    dict_node *entry, auxentry;

    if (add(key, val) == REDIS_OK)
        return 1;

    entry = find(key);
    auxentry = *entry;

    entry->_val = val;
    if (_free_value_fn) {
        _free_value_fn(auxentry._val);
    }
    return 0;
}

dict_node* dict::rep::replace_raw(const redis_key& key)
{
    dict_node *entry = find(key);
    return entry ? entry : add_raw(key);
}

int dict::rep::generic_delete(const redis_key& key, int nofree)
{
    unsigned int h, idx;
    dict_node *he, *prevHe;
    int table;

    if (_ht[0]._size == 0) return REDIS_ERR;
    if (dict_is_rehashing()) do_rehash_step();
    h = key.hash();

    for (table = 0; table <= 1; table++) {
        idx = h & _ht[table]._size_mask;
        he = _ht[table]._table[idx];
        prevHe = nullptr;
        while(he) {
            if (key_equal(&key.key(), key.hash(), he->_val)) {
                if (prevHe)
                    prevHe->_next = he->_next;
                else
                    _ht[table]._table[idx] = he->_next;
                if (!nofree) {
                    if (_free_value_fn != nullptr) _free_value_fn(he->_val);
                }
                delete he;
                _ht[table]._used--;
                return REDIS_OK;
            }
            prevHe = he;
            he = he->_next;
        }
        if (!dict_is_rehashing()) break;
    }
    return REDIS_ERR;
}

int dict::rep::remove(const redis_key& key) {
    return generic_delete(key, 0);
}

int dict::rep::remove_no_free(const redis_key& key) {
    return generic_delete(key, 1);
}

std::vector<item_ptr> dict::rep::fetch(const std::unordered_set<sstring>& keys)
{
    std::vector<item_ptr> items;
    dict_iterator iter(this);
    iter.seek_to_first();
    while (iter.status() == REDIS_OK) {
        auto n = iter.value();
        auto k = n->_val->key();
        if (std::find_if(keys.begin(), keys.end(), [this, &k] (const sstring& key) { return key_equal(k, key); }) != keys.end()) {
            items.emplace_back(item_ptr(n->_val));
        }
        iter.next();
    }
    return std::move(items);
}

std::vector<item_ptr> dict::rep::fetch()
{
    std::vector<item_ptr> items;
    dict_iterator iter(this);
    iter.seek_to_first();
    while (iter.status() == REDIS_OK) {
        auto n = iter.value();
        items.emplace_back(item_ptr(n->_val));
        iter.next();
    }
    return std::move(items);
}

int dict::rep::clear(dict_hash_table *ht)
{
    unsigned long i;

    for (i = 0; i < ht->_size && ht->_used > 0; i++) {
        dict_node *he, *nextHe;

        if ((he = ht->_table[i]) == nullptr) continue;
        while(he) {
            nextHe = he->_next;
            if (_free_value_fn != nullptr) _free_value_fn(he->_val);
            delete he;

            ht->_used--;
            he = nextHe;
        }
    }
    delete[] ht->_table;
    hash_table_reset(ht);
    return REDIS_OK;
}

dict::rep::~rep()
{
    clear(&_ht[0]);
    clear(&_ht[1]);
}

dict_node* dict::rep::find(const redis_key& key)
{
    dict_node *he;
    size_t h, idx, table;

    if (_ht[0]._used + _ht[1]._used == 0) return nullptr;
    if (dict_is_rehashing()) do_rehash_step();
    h = key.hash(); 
    for (table = 0; table <= 1; table++) {
        idx = h & _ht[table]._size_mask;
        he = _ht[table]._table[idx];
        while(he) {
            if (key_equal(&key.key(), key.hash(), he->_val)) {
                return he;
            }
            he = he->_next;
        }
        if (!dict_is_rehashing()) return nullptr;
    }
    return nullptr;
}

item* dict::rep::fetch_value(const redis_key& key)
{
    dict_node *he;
    he = find(key);
    return he ? he->_val : nullptr;
}

int dict::rep::expend_if_needed()
{
    if (dict_is_rehashing()) return REDIS_OK;

    if (_ht[0]._size == 0) return expand_room(DICT_HT_INITIAL_SIZE);

    if (_ht[0]._used >= _ht[0]._size &&
            (dict_can_resize ||
             _ht[0]._used/_ht[0]._size > dict_force_resize_ratio))
    {
        return expand_room(_ht[0]._used*2);
    }
    return REDIS_OK;
}

unsigned long dict::rep::dict_next_size(unsigned long size)
{
    unsigned long i = DICT_HT_INITIAL_SIZE;

    if (size >= LONG_MAX) return LONG_MAX;
    while(1) {
        if (i >= size)
            return i;
        i *= 2;
    }
}

int dict::rep::key_index(const redis_key& key)
{
    unsigned int h, idx, table;
    dict_node *he;

    if (expend_if_needed() == REDIS_ERR)
        return -1;
    h = key.hash();
    for (table = 0; table <= 1; table++) {
        idx = h & _ht[table]._size_mask;
        he = _ht[table]._table[idx];
        while(he) {
            if (key_equal(&key.key(), key.hash(), he->_val) == true)
                return -1;
            he = he->_next;
        }
        if (!dict_is_rehashing()) break;
    }
    return idx;
}

void dict::rep::dict_release()
{
    clear(&_ht[0]);
    clear(&_ht[1]);
    _rehash_idx = -1;
    _iterators = 0;
}

// API
dict::dict() : _rep(new dict::rep())
{
}

dict::~dict()
{
    if (_rep != nullptr) {
        delete _rep;
    }
}

int dict::set(const redis_key& key, item* val)
{
    return _rep->add(key, val);
}

int dict::replace(const redis_key& key, item* val)
{
    return _rep->replace(key, val);
}

int dict::remove(const redis_key& key)
{
    return _rep->remove(key);
}

int dict::exists(const redis_key& key)
{
    return _rep->find(key) != nullptr ? 1 : 0; 
}

size_t dict::size()
{
  return _rep->size();
}

item* dict::fetch_raw(const redis_key& key)
{
    return _rep->fetch_value(key);
}

item_ptr dict::fetch(const redis_key& key)
{
    return item_ptr(_rep->fetch_value(key));
}

std::vector<item_ptr> dict::fetch(const std::unordered_set<sstring>& keys)
{
  return _rep->fetch(keys);
}

std::vector<item_ptr> dict::fetch()
{
  return _rep->fetch();
}

} /* namespace redis*/
