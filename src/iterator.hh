
#ifndef _ITERATOR_HH 
#define _ITERATOR_HH 
#include "core/sstring.hh"
namespace redis {
class item;
template<typename ValueType>
class iterator {
 public:
  iterator() {}
  virtual ~iterator() {}

  virtual bool valid() const = 0;

  virtual void seek_to_first() = 0;

  virtual void seek_to_last() = 0;

  virtual void seek(const sstring& key) = 0;

  virtual void next() = 0;

  virtual void prev() = 0;

  virtual sstring* key() const = 0;

  virtual  ValueType* value() const = 0;

  virtual int status() const = 0;


 private:
  iterator(const iterator&);
  void operator=(const iterator&);
};

}  // namespace redis

#endif
