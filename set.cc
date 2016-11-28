#include "set.hh"
#include "iterator.hh"
#include <boost/intrusive/set.hpp>
namespace redis {

struct entry {
    boost::intrusive::set_member_hook<> _link;
    item* _value;
    entry(item* value) : _value(value) {}
    entry() : _value(nullptr) {}
    struct compare {
    bool operator () (const entry& l, const entry& r) const {
        return true;
    }
    };
};

struct set::rep {
    using redis_set = boost::intrusive::set<entry,
          boost::intrusive::member_hook<entry, boost::intrusive::set_member_hook<>, &entry::_link>,
          boost::intrusive::compare<entry::compare>>;

    redis_set _set;
    int insert(item* e) {
        entry entry_{ e };
        _set.insert(entry_);
        return REDIS_OK;
    }
    int erase(sstring& e) {
        entry entry_{};
        return _set.erase(entry_) > 0 ? REDIS_OK : REDIS_NONE;
    }
    int exist(sstring& e) {
        entry entry_{};
        return _set.find(entry_) != _set.end() ? REDIS_OK : REDIS_NONE;
    }
};

set::set() : _rep (new set::rep())
{
}

set::~set()
{
    if (_rep != nullptr) {
        delete _rep;
    }
}

int set::insert(item* item) {
    return _rep->insert(item);
}

int set::erase(sstring& e) {
    return _rep->erase(e);
}

int set::exist(sstring& e) {
    return _rep->exist(e);
}

}
