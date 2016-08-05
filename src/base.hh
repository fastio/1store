#ifndef _OBJECT_HH
#define _OBJECT_HH

#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <iomanip>
#include <sstream>
#include <vector>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include <unistd.h>

namespace redis {

namespace stdx = std::experimental;


enum {
  FLAG_SET_EX = 1 << 0,
  FLAG_SET_PX = 1 << 1,
  FLAG_SET_NX = 1 << 2,
  FLAG_SET_XX = 1 << 3,
};

enum {
  REDIS_RAW_UINT64,
  REDIS_RAW_INT64,
  REDIS_RAW_DOUBLE,
  REDIS_RAW_STRING,
  REDIS_RAW_OBJECT, // for data struct
  REDIS_RAW_ITEM,   // for data item
  REDIS_LIST,
};

struct args_collection {
  uint32_t _command_args_count;
  sstring _command;
  std::vector<sstring> _command_args;
  args_collection () : _command_args_count(0) {}
};


class item;
extern __thread slab_allocator<item>* _slab;
inline slab_allocator<item>& local_slab() {
  return *_slab;
}
class redis_commands;
extern __thread redis_commands* _redis_commands_ptr;
inline redis_commands* redis_commands_ptr() {
  return _redis_commands_ptr;
}
class object
{
public:
  object() {}
  virtual ~object() {};
};

class item : public slab_item_base {
private:
    uint32_t _value_size;
    uint32_t _slab_page_index;
    uint16_t _ref_count;
    uint8_t  _type;
    uint8_t  _cpu_id;
    long     _expire;

    union {
      char     _data[];
      object*    _ptr;
      double   _double;
      uint64_t _uint64;
      int64_t  _int64;
    } _u;
    friend class dict;
    static constexpr uint32_t field_alignment = alignof(void*);
public:
    inline static size_t item_size_for_row_string(const sstring& val) {
       return sizeof (_value_size) + sizeof(_slab_page_index) + sizeof(_ref_count)
         + sizeof(_type) + sizeof(_expire) + val.size() + sizeof(uint8_t);
    }
    inline static size_t item_size_for_row_string_append(const sstring& val, const std::experimental::string_view& v) {
       return sizeof (_value_size) + sizeof(_slab_page_index) + sizeof(_ref_count)
         + sizeof(_type) + sizeof(_expire) + val.size() + sizeof(uint8_t) + v.size();
    }
    inline static size_t item_size_for_list() {
       return sizeof (_value_size) + sizeof(_slab_page_index) + sizeof(_ref_count)
         + sizeof(_type) + sizeof(_expire) + sizeof(uint8_t) + sizeof(void*);
    }
    inline static size_t item_size_for_uint64() {
       return sizeof (_value_size) + sizeof(_slab_page_index) + sizeof(_ref_count)
         + sizeof(_type) + sizeof(_expire) + sizeof(uint8_t) + sizeof(uint64_t);
    }
private:

    inline void encode_u64(char* b, uint64_t d) {
      b[0] = d & 0xff;
      b[1] = (d >> 8) & 0xff;
      b[2] = (d >> 16) & 0xff;
      b[3] = (d >> 24) & 0xff;
      b[4] = (d >> 32) & 0xff;
      b[5] = (d >> 40) & 0xff;
      b[6] = (d >> 48) & 0xff;
      b[7] = (d >> 56) & 0xff;
    }

    inline uint32_t decode_u32(const char* b) {
      return ((static_cast<uint32_t>(static_cast<unsigned char>(b[0])))
          | (static_cast<uint32_t>(static_cast<unsigned char>(b[1])) << 8)
          | (static_cast<uint32_t>(static_cast<unsigned char>(b[2])) << 16)
          | (static_cast<uint32_t>(static_cast<unsigned char>(b[3])) << 24));
    }
    inline uint64_t decode_u64(const char* b) {
      return (static_cast<uint64_t>(decode_u32(b + 4)) << 32) | decode_u32(b);
    }
    inline int32_t decode_i32(const char* b) {
      return ((static_cast<int32_t>(static_cast<char>(b[0])))
          | (static_cast<int32_t>(static_cast<char>(b[1])) << 8)
          | (static_cast<int32_t>(static_cast<char>(b[2])) << 16)
          | (static_cast<int32_t>(static_cast<char>(b[3])) << 24));
    }
    inline int64_t decode_i64(const char* b) {
      return (static_cast<int64_t>(decode_i32(b + 4)) << 32) | decode_i32(b);
    }
    inline void encode_ptr(char* b, void* o) {
      encode_u64(b, reinterpret_cast<uint64_t>(o));
    }
    inline void* decode_ptr(const char* b) {
      return reinterpret_cast<void*>(decode_u64(b));
    }
public:
    item(uint32_t slab_page_index, sstring&& value, long expire)
      : _value_size(value.size())
      , _slab_page_index(slab_page_index)
      , _ref_count(0U)
      , _type(REDIS_RAW_STRING)
      , _cpu_id(engine().cpu_id())
      , _expire(expire)
    {
      memcpy(_u._data, value.c_str(), _value_size);
    }

    item(uint32_t slab_page_index, const std::experimental::string_view& value, sstring&& append, long expire)
      : _value_size(value.size())
      , _slab_page_index(slab_page_index)
      , _ref_count(0U)
      , _type(REDIS_RAW_STRING)
      , _cpu_id(engine().cpu_id())
      , _expire(expire)
    {
      memcpy(_u._data, value.data(), _value_size);
      if (append.size() > 0) {
        memcpy(_u._data + _value_size, append.c_str(), append.size());
        _value_size += append.size();
      }
    }

    item(uint32_t slab_page_index, sstring&& value)
      : _value_size(value.size())
      , _slab_page_index(slab_page_index)
      , _ref_count(0U)
      , _type(REDIS_RAW_ITEM)
      , _cpu_id(engine().cpu_id())
      , _expire(0)
    {
      memcpy(_u._data, value.c_str(), _value_size);
    }

    item(uint32_t slab_page_index, uint64_t value)
      : _value_size(sizeof(uint64_t))
      , _slab_page_index(slab_page_index)
      , _ref_count(0U)
      , _type(REDIS_RAW_UINT64)
      , _cpu_id(engine().cpu_id())
      , _expire(0)
    {
      _u._uint64 = value;
    }

    item(uint32_t slab_page_index, object* ptr, uint8_t type, long expire)
      : _value_size(sizeof(void*))
      , _slab_page_index(slab_page_index)
      , _ref_count(0U)
      , _type(type)
      , _cpu_id(engine().cpu_id())
      , _expire(expire)
    {
      _u._ptr = ptr;
    }


    item(const item&) = delete;
    item(item&&) = delete;


    inline const std::experimental::string_view value() const {
      return std::experimental::string_view(_u._data, _value_size);
    }
    
    void* ptr() { return _u._ptr; }

    uint64_t uint64() { return _u._uint64; }
    uint64_t incr(uint64_t step) { _u._uint64 += step; return _u._uint64; }
    uint64_t decr(uint64_t step) { _u._uint64 -= step; return _u._uint64; }

    inline const uint32_t value_size() const { return _value_size; }

    inline uint32_t get_slab_page_index() const {
      return _slab_page_index;
    }
    inline bool is_unlocked() const {
      return _ref_count == 1;
    }
    inline const char* data() const { return _u._data; }
    inline uint8_t type() const { return _type; }
    friend inline void intrusive_ptr_add_ref(item* it) {
      assert(it->_ref_count >= 0);
      ++it->_ref_count;
      if (it->_ref_count == 2) {
        local_slab().lock_item(it);
      }
    }

    friend inline void intrusive_ptr_release(item* it) {
      --it->_ref_count;
      if (it->_ref_count == 1) {
        local_slab().unlock_item(it);
      } else if (it->_ref_count == 0) {
        if (it->_type == REDIS_LIST) {
          delete it->_u._ptr;
        }
        local_slab().free(it);
      }
      assert(it->_ref_count >= 0);
    }
};
static constexpr const char* msg_crlf = "\r\n";
static constexpr const char* msg_ok = "+OK\r\n";
static constexpr const char* msg_err = "-ERR\r\n";
//shared.emptybulk = createObject(OBJ_STRING,sdsnew("$0\r\n\r\n"));
static constexpr const char* msg_zero = ":0\r\n";
static constexpr const char* msg_one = ":1\r\n";
static constexpr const char* msg_neg_one = ":-1\r\n";
static constexpr const char* msg_null_blik = "$-1\r\n";
static constexpr const char* msg_null_multi_bulk = "*-1\r\n";
static constexpr const char* msg_empty_multi_bulk = "*0\r\n";
static constexpr const char* msg_type_err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
static constexpr const char* msg_nokey_err = "-ERR no such key\r\n";
static constexpr const char* msg_syntax_err = "-ERR syntax error\r\n";
static constexpr const char* msg_same_object_err = "-ERR source and destination objects are the same\r\n";
static constexpr const char* msg_out_of_range_err = "-ERR index out of range\r\n";

static constexpr const char *msg_str_tag = "+";
static constexpr const char *msg_num_tag = ":";
static constexpr const char *msg_sigle_tag = "*";
static constexpr const char *msg_batch_tag = "$";
static constexpr const char *msg_not_found = "+(nil)\r\n";
static constexpr const char *msg_nil = "+(nil)\r\n";
static constexpr const int REDIS_OK = 0;
static constexpr const int REDIS_ERR = 1;
} /* namespace redis */


#endif
