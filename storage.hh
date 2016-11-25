#pragma once
#include "dict.hh"
namespace redis {

struct remote_origin_tag {
    template <typename T>
    static inline
    T move_if_local(T& ref) {
        return ref;
    }
};

struct local_origin_tag {
    template <typename T>
    static inline
    T move_if_local(T& ref) {
        return std::move(ref);
    }
};

class storage {
public:
  storage(const sstring& name, dict* store) : name_(name), _store(store)
  {
  }
  virtual ~storage()
  {
  }
protected:
  sstring name_;
  dict* _store;
};
}
