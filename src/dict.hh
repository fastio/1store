#ifndef _DICT_HH
#define _DICT_HH
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
class dict_iterator;
using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;
class dict : public object {
private:
  friend class dict_iterator;
  struct rep;
  rep* _rep;
public:
  dict();
  virtual ~dict();
  int set(const sstring& key, size_t kh, item* val);
  int exists(const sstring& key, size_t kh);
  item_ptr fetch(const sstring& key, size_t kh);
  item* fetch_raw(const sstring& key, size_t kh);
  int replace(const sstring& key, size_t kh, item* val);
  int remove(const sstring& key, size_t kh);
};

} // namespace redis
#endif /* __DICT_H */
