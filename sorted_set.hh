#pragma once
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/shared_ptr.hh"
#include "core/sharded.hh"
#include "base.hh"
namespace redis {
using item_ptr = foreign_ptr<lw_shared_ptr<item>>;
class item;
class sorted_set_iterator;
class sorted_set {
private:
    friend class sorted_set_iterator;
    struct rep;
    rep* _rep;
public:
    sorted_set();
    ~sorted_set();
    bool exists(const redis_key& key) { return false; }
    int insert(const redis_key& key, lw_shared_ptr<item> item);
    size_t size();
    size_t count(double min, double max);
    double incrby(const redis_key& key, double delta);
    std::vector<item_ptr> range_by_rank(size_t begin, size_t end, bool reverse);
    std::vector<item_ptr> range_by_score(double min, double max, bool reverse);
};
}
