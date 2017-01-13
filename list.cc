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
 *
 *  Copyright (c) 2016-2026, Peng Jian, pstack@163.com. All rights reserved.
 *
 */
#include "list.hh"
#include "iterator.hh"
#include <functional>
#include <iomanip>
#include <cstring>
namespace redis {

struct list_node
{
    struct list_node* _prev;
    struct list_node* _next;
    item* _value;
    list_node() : _prev(nullptr), _next(nullptr), _value(nullptr) {}
    ~list_node()
    {
        if (!_value) {
           intrusive_ptr_release(_value);
        }
    }
};

struct list::rep
{
    friend class list;
    list_node *_head;
    list_node *_tail;
    unsigned long _len;

    std::function<void(item* val)> _free_value_fn;
    rep() : _head(nullptr), _tail(nullptr), _len(0) {}
    ~rep();
    int add_node_head(item *value);
    int add_node_tail(item *value);
    void rotate();
    item* index(long index);
    item* pop_head();
    item* pop_tail();
    int insert_node(list_node* n, item *value, int after);
    void del_node(list_node *node);
    item* remove_node(list_node *node);
    list_node* search_key(const sstring& key);
    bool node_equal(list_node* n, const sstring& key) {
        if (n != nullptr && n->_value != nullptr) {
            const auto& k = n->_value->value();
            return k.size() == key.size() && memcmp(k.data(), key.c_str(), key.size()) == 0;
        }
        return false;
    }
    int set(long idx, item* value);
    std::vector<item_ptr> range(int start, int end);
    int trim(int start, int end);
    int trem(int count, sstring& value);
    long length() { return _len; }
    void remove_range(int start, int count);
};

list::list() : _rep(new list::rep()) {}

list::~list()
{
    if (_rep != nullptr) {
        delete _rep;
    }
}

long list::length()
{
    return _rep->length();
}

int list::add_head(item* val)
{
    return _rep->add_node_head(val);
}

item_ptr list::pop_head()
{
    return item_ptr(_rep->pop_head());
}

int list::add_tail(item* val)
{
    return _rep->add_node_tail(val);
}

item_ptr list::pop_tail()
{
    return item_ptr(_rep->pop_tail());
}

int list::set(long idx, item* value) {
    return _rep->set(idx, value);
}

int list::insert_after(const sstring& pivot, item *value)
{
    auto n = _rep->search_key(pivot);
    if (n == nullptr) {
        return REDIS_ERR;
    }
    return _rep->insert_node(n, value, 1);
}

int list::insert_before(const sstring& pivot, item *value)
{
    auto n = _rep->search_key(pivot);
    if (n == nullptr) {
        return REDIS_ERR;
    }
    return _rep->insert_node(n, value, 0);
}

void list::remove(const sstring& target)
{
    auto n = _rep->search_key(target);
    if (n != nullptr) {
        _rep->del_node(n);
    }
}
std::vector<item_ptr> list::range(int start, int end)
{
    return _rep->range(start, end);
}
item_ptr list::index(long idx)
{
    return item_ptr(_rep->index(idx));
}
int list::trem(int count, sstring& value)
{
    return _rep->trem(count, value);
}
int list::trim(int start, int stop) 
{
    return _rep->trim(start, stop);
}
enum
{
    FROM_HEAD_TO_TAIL = 0,
    FROM_TAIL_TO_HEAD = 1
};

class list_iterator : public iterator<list_node>
{
private:
    int _direction;
    list_node* _next;
    list::rep* _rep;
public:
    list_iterator(list::rep* rep, int dir) : _direction(dir), _next(nullptr), _rep(rep) {
        if (dir == FROM_HEAD_TO_TAIL)
            _next = rep->_head;
        else
            _next = rep->_tail;
    }
    ~list_iterator() { }

    bool valid() const { return _next != nullptr; }
    void seek_to_first() {
        if (_direction == FROM_HEAD_TO_TAIL) {
            _next = _rep->_head;
        } else {
            _next = _rep->_tail;
        }
    }
    void seek_to_last() {
        if (_direction == FROM_HEAD_TO_TAIL) {
            _next = _rep->_tail;
        } else {
            _next = _rep->_head;
        }
    }
    void seek(const sstring& key) {
    }
    void next() {
        if (_next != nullptr)
            _next = _direction == FROM_HEAD_TO_TAIL ? _next->_next : _next->_prev;
    }
    void prev() {
        if (_next != nullptr)
            _next = _direction == FROM_HEAD_TO_TAIL ? _next->_prev : _next->_next;
    }
    list_node* value() const {
        if (_next != nullptr) {
            return _next;
        }
        return nullptr;
    }
    int status() const {
        return _next != nullptr ? REDIS_OK : REDIS_ERR;
    }
};

list::rep::~rep()
{
    list_node *current, *next;

    current = _head;
    while(_len--) {
        next = current->_next;
        if (_free_value_fn != nullptr) _free_value_fn(current->_value);
        delete current;
        current = next;
    }
}

int list::rep::add_node_head(item *value)
{
    list_node *node = new list_node();

    node->_value = value;
    if (_len == 0) {
        _head = _tail = node;
        node->_prev = node->_next = nullptr;
    } else {
        node->_prev = nullptr;
        node->_next = _head;
        _head->_prev = node;
        _head = node;
    }
    _len++;
    return REDIS_OK;
}

int list::rep::add_node_tail(item *value)
{
    list_node *node = new list_node();

    node->_value = value;
    if (_len == 0) {
        _head = _tail = node;
        node->_prev = node->_next = nullptr;
    } else {
        node->_prev = _tail;
        node->_next = nullptr;
        _tail->_next = node;
        _tail = node;
    }
    _len++;
    return REDIS_OK;
}

int list::rep::insert_node(list_node *pivot, item *value, int after)
{
    list_node *node = new list_node();

    node->_value = value;
    if (after) {
        node->_prev = pivot;
        node->_next = pivot->_next;
        if (_tail == pivot) {
            _tail = node;
        }
    } else {
        node->_next = pivot;
        node->_prev = pivot->_prev;
        if (_head == pivot) {
            _head = node;
        }
    }
    if (node->_prev != nullptr) {
        node->_prev->_next = node;
    }
    if (node->_next != nullptr) {
        node->_next->_prev = node;
    }
    _len++;
    return REDIS_OK;
}

void list::rep::del_node(list_node *node)
{
    if (node->_prev)
        node->_prev->_next = node->_next;
    else
        _head = node->_next;
    if (node->_next)
        node->_next->_prev = node->_prev;
    else
        _tail = node->_prev;
    if (_free_value_fn != nullptr) _free_value_fn(node->_value);
    delete node;
    _len--;
}

item* list::rep::remove_node(list_node *node)
{
    if (node->_prev)
        node->_prev->_next = node->_next;
    else
        _head = node->_next;
    if (node->_next)
        node->_next->_prev = node->_prev;
    else
        _tail = node->_prev;
    _len--;
    auto i = node != nullptr ? node->_value : nullptr;
    delete node;
    return i;
}

void list::rep::remove_range(int start, int count)
{
    if (count <= 0)
        return;

    while (start < 0) start += _len;
    list_iterator iter(this, FROM_HEAD_TO_TAIL);
    iter.seek_to_first();
    while (iter.status() == REDIS_OK && start-- > 0) {
        iter.next();
    }
    while (iter.status() == REDIS_OK && count-- > 0) {
        auto n = iter.value();
        iter.next();
        del_node(n);
    }
}

int list::rep::trim(int start, int end)
{
    if (_len == 0)
        return REDIS_ERR;
    int lr = 0, rr = 0;
    if (start < 0) start += _len;
    if (end < 0) end += _len;

    if (end < 0)
        return REDIS_ERR;

    if (start < 0) start = 0;
    if (start > end || start >= static_cast<int>(_len)) {
        lr = _len; // all nodes were removed
        rr = 0;
    }
    else {
        if (end > static_cast<int>(_len)) end = _len - 1;
        lr = start;
        rr = _len - end - 1;
    }

    remove_range(0, lr);
    remove_range(-rr, rr);
    return REDIS_OK;
}

int list::rep::trem(int count, sstring& value)
{
    if (_len == 0)
        return 0;

    int removed = 0;
    list_iterator iter(this, count > 0 ? FROM_HEAD_TO_TAIL : FROM_TAIL_TO_HEAD);

    if (count < 0)
        count = -count;

    // remove all nodes
    if (count == 0)
        count = static_cast<int>(_len);

    iter.seek_to_first();
    while (iter.status() == REDIS_OK && count > 0) {
        if (node_equal(iter.value(), value) == true) {
            auto n = iter.value();
            iter.next();
            del_node(n);
            count--;
            removed++;
        }
        else {
            iter.next();
        }
    }
    return removed;
}

std::vector<item_ptr> list::rep::range(int start, int end)
{
    if (_len == 0) {
        return std::move(std::vector<item_ptr>());
    }
    if (start < 0) { start += _len; }
    if (end < 0) { end += _len; }
    if (start < 0) start = 0;
    if (start > end) {
        return std::move(std::vector<item_ptr>());
    }
    int count = end - start + 1;
    list_iterator iter(this, FROM_HEAD_TO_TAIL);
    iter.seek_to_first();
    while (start-- > 0) iter.next();
    std::vector<item_ptr> result;
    while (iter.status() == REDIS_OK && count-- > 0) {
        auto n = iter.value();
        if (n && n->_value)
            result.emplace_back(item_ptr(n->_value));
        iter.next();
    }
    return std::move(result);
}

list_node* list::rep::search_key(const sstring& key)
{
    list_iterator iter(this, FROM_HEAD_TO_TAIL);
    iter.seek_to_first();
    while (iter.status() == REDIS_OK) {
        if (node_equal(iter.value(), key) == true) {
            return iter.value();
        }
        iter.next();
    }
    return nullptr;
}

item* list::rep::pop_head()
{
    item* n = nullptr;
    if (_head) {
        n = remove_node(_head);
    }
    return n;
}

item* list::rep::pop_tail()
{
    item* n = nullptr;
    if (_tail) {
        n = remove_node(_tail);
    }
    return n;
}

int list::rep::set(long idx, item* value)
{
    list_node *n;

    if (idx < 0) {
        idx = (-idx) - 1;
        if (idx < 0) return REDIS_ERR;
        n = _tail;
        while(idx-- && n) n = n->_prev;
    } else {
        n = _head;
        if (static_cast<unsigned long>(idx) > _len) return REDIS_ERR;
        while(idx-- && n) n = n->_next;
    }
    if (n) {
        auto old = n->_value;
        n->_value = value;
        if (_free_value_fn != nullptr) _free_value_fn(old);
        return REDIS_OK;
    }
    return REDIS_ERR;
}

item* list::rep::index(long idx)
{
    list_node *n;

    if (idx < 0) {
        idx = (-idx) - 1;
        n = _tail;
        while(idx-- && n) n = n->_prev;
    } else {
        n = _head;
        while(idx-- && n) n = n->_next;
    }
    return n->_value;
}

void list::rep::rotate()
{
    list_node *t = _tail;

    if (_len <= 1) return;

    _tail = t->_prev;
    _tail->_next = nullptr;

    _head->_prev = t;
    t->_prev = nullptr;
    t->_next = _head;
    _head = t;
}

} /* redis node */
