#include "base.hh"
#include "list.hh"
#include "dict.hh"
namespace redis {
item::~item()
{
    if (_type == REDIS_LIST) {
        delete list_ptr();
    }
    else if (_type == REDIS_DICT || _type == REDIS_SET) {
        delete dict_ptr();
    }
    if (_appends) delete[] _appends;
}
}
