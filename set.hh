#pragma once
#include "core/stream.hh"
#include "core/memory.hh"
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include "core/shared_ptr.hh"
#include "core/sharded.hh"
#include "base.hh"
namespace redis {
class item;
class set_iterator;
class set : public object {
public:
    set();
    ~set();
    int insert(item* item);
    int erase(sstring& e);
    int exist(sstring& e);
private:
    friend class set_iterator;
    struct rep;
    rep* _rep;
};
}
