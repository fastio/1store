#ifndef _LIST_HH
#define _LIST_HH
#include "core/stream.hh"
#include "core/memory.hh"
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include "core/shared_ptr.hh"
#include "core/sharded.hh"
#include "base.hh"
#include <vector>
namespace redis {
class item;
using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;

class list_iterator;

class list : public object {
private:
  friend class list_iterator;
  struct rep;
  rep* _rep;
public:
  list();
  ~list();
  int add_head(item* val);
  int add_tail(item* val);
  item_ptr pop_head();
  item_ptr pop_tail();
  int insert_before(const sstring& pivot, item *value);
  int insert_after(const sstring& pivot, item *value);
  int set(long idx, item *value);
  void remove(const sstring& target);
  item_ptr index(long index);
  std::vector<item_ptr> range(int start, int end);
  int trim(int start, int end);
  long length();
};
}
#endif 
