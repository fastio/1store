/*
 * Copyright (C) 2014 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <experimental/optional>
#include <boost/functional/hash.hpp>
#include <iosfwd>
#include <sstream>

#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "utils/UUID.hh"
#include "net/byteorder.hh"
#include "bytes.hh"
#include "log.hh"
#include "to_string.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/numeric.hpp>
#include <boost/range/combine.hpp>
#include "net/ip.hh"
#include "hashing.hh"
#include <boost/multiprecision/cpp_int.hpp>  // FIXME: remove somehow
/*
class tuple_type_impl;
class big_decimal;

namespace cql3 {

class cql3_type;
class column_specification;
shared_ptr<cql3_type> make_cql3_tuple_type(shared_ptr<const tuple_type_impl> t);

}

// Like std::lexicographical_compare but injects values from shared sequence (types) to the comparator
// Compare is an abstract_type-aware less comparator, which takes the type as first argument.
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Compare>
bool lexicographical_compare(TypesIterator types, InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2, Compare comp) {
    while (first1 != last1 && first2 != last2) {
        if (comp(*types, *first1, *first2)) {
            return true;
        }
        if (comp(*types, *first2, *first1)) {
            return false;
        }
        ++first1;
        ++first2;
        ++types;
    }
    return (first1 == last1) && (first2 != last2);
}

// Like std::lexicographical_compare but injects values from shared sequence
// (types) to the comparator. Compare is an abstract_type-aware trichotomic
// comparator, which takes the type as first argument.
//
// A trichotomic comparator returns an integer which is less, equal or greater
// than zero when the first value is respectively smaller, equal or greater
// than the second value.
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Compare>
int lexicographical_tri_compare(TypesIterator types_first, TypesIterator types_last,
        InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2,
        Compare comp) {
    while (types_first != types_last && first1 != last1 && first2 != last2) {
        auto c = comp(*types_first, *first1, *first2);
        if (c) {
            return c;
        }
        ++first1;
        ++first2;
        ++types_first;
    }
    bool e1 = first1 == last1;
    bool e2 = first2 == last2;
    if (e1 != e2) {
        return e2 ? 1 : -1;
    }
    return 0;
}

// Trichotomic version of std::lexicographical_compare()
//
// Returns an integer which is less, equal or greater than zero when the first value
// is respectively smaller, equal or greater than the second value.
template <typename InputIt1, typename InputIt2, typename Compare>
int lexicographical_tri_compare(InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2,
        Compare comp) {
    while (first1 != last1 && first2 != last2) {
        auto c = comp(*first1, *first2);
        if (c) {
            return c;
        }
        ++first1;
        ++first2;
    }
    bool e1 = first1 == last1;
    bool e2 = first2 == last2;
    if (e1 != e2) {
        return e2 ? 1 : -1;
    }
    return 0;
}

// A trichotomic comparator for prefix equality total ordering.
// In this ordering, two sequences are equal iff any of them is a prefix
// of the another. Otherwise, lexicographical ordering determines the order.
//
// 'comp' is an abstract_type-aware trichotomic comparator, which takes the
// type as first argument.
//
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Compare>
int prefix_equality_tri_compare(TypesIterator types, InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2, Compare comp) {
    while (first1 != last1 && first2 != last2) {
        auto c = comp(*types, *first1, *first2);
        if (c) {
            return c;
        }
        ++first1;
        ++first2;
        ++types;
    }
    return 0;
}

// Returns true iff the second sequence is a prefix of the first sequence
// Equality is an abstract_type-aware equality checker which takes the type as first argument.
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Equality>
bool is_prefixed_by(TypesIterator types, InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2, Equality equality) {
    while (first1 != last1 && first2 != last2) {
        if (!equality(*types, *first1, *first2)) {
            return false;
        }
        ++first1;
        ++first2;
        ++types;
    }
    return first2 == last2;
}
*/
class marshal_exception : public std::exception {
    sstring _why;
public:
    marshal_exception() : _why("marshalling error") {}
    marshal_exception(sstring why) : _why(sstring("marshaling error: ") + why) {}
    virtual const char* what() const noexcept override { return _why.c_str(); }
};

struct runtime_exception : public std::exception {
    sstring _why;
public:
    runtime_exception(sstring why) : _why(sstring("runtime error: ") + why) {}
    virtual const char* what() const noexcept override { return _why.c_str(); }
};

inline int32_t compare_unsigned(bytes_view v1, bytes_view v2) {
    auto n = memcmp(v1.begin(), v2.begin(), std::min(v1.size(), v2.size()));
    if (n) {
        return n;
    }
    return (int32_t) (v1.size() - v2.size());
}

struct empty_t {};

class empty_value_exception : public std::exception {
public:
    virtual const char* what() const noexcept override {
        return "Unexpected empty value";
    }
};
/*
// Cassandra has a notion of empty values even for scalars (i.e. int).  This is
// distinct from NULL which means deleted or never set.  It is serialized
// as a zero-length byte array (whereas NULL is serialized as a negative-length
// byte array).
template <typename T>
class emptyable {
    // We don't use optional<>, to avoid lots of ifs during the copy and move constructors
    static_assert(std::is_default_constructible<T>::value, "must be default constructible");
    bool _is_empty = false;
    T _value;
public:
    // default-constructor defaults to a non-empty value, since empty is the
    // exception rather than the rule
    emptyable() : _value{} {}
    emptyable(const T& x) : _value(x) {}
    emptyable(T&& x) : _value(std::move(x)) {}
    emptyable(empty_t) : _is_empty(true) {}
    template <typename... U>
    emptyable(U&&... args) : _value(std::forward<U>(args)...) {}
    bool empty() const { return _is_empty; }
    operator const T& () const { verify(); return _value; }
    operator T&& () && { verify(); return std::move(_value); }
    const T& get() const & { verify(); return _value; }
    T&& get() && { verify(); return std::move(_value); }
private:
    void verify() const {
        if (_is_empty) {
            throw empty_value_exception();
        }
    }
};

template <typename T>
inline
bool
operator==(const emptyable<T>& me1, const emptyable<T>& me2) {
    if (me1.empty() && me2.empty()) {
        return true;
    }
    if (me1.empty() != me2.empty()) {
        return false;
    }
    return me1.get() == me2.get();
}

template <typename T>
inline
bool
operator<(const emptyable<T>& me1, const emptyable<T>& me2) {
    if (me1.empty()) {
        if (me2.empty()) {
            return false;
        } else {
            return true;
        }
    }
    if (me2.empty()) {
        return false;
    } else {
        return me1.get() < me2.get();
    }
}

// Checks whether T::empty() const exists and returns bool
template <typename T>
class has_empty {
    template <typename X>
    constexpr static auto check(const X* x) -> std::enable_if_t<std::is_same<bool, decltype(x->empty())>::value, bool> {
        return true;
    }
    template <typename X>
    constexpr static auto check(...) -> bool {
        return false;
    }
public:
    constexpr static bool value = check<T>(nullptr);
};

template <typename T>
using maybe_empty =
        std::conditional_t<has_empty<T>::value, T, emptyable<T>>;

class abstract_type;

class serialized_compare;
class serialized_tri_compare;
class user_type_impl;

inline
bytes
to_bytes(const char* x) {
    return bytes(reinterpret_cast<const int8_t*>(x), std::strlen(x));
}

// FIXME: make more explicit
inline
bytes
to_bytes(const std::string& x) {
    return bytes(reinterpret_cast<const int8_t*>(x.data()), x.size());
}

inline
bytes_view
to_bytes_view(const std::string& x) {
    return bytes_view(reinterpret_cast<const int8_t*>(x.data()), x.size());
}

inline
bytes
to_bytes(bytes_view x) {
    return bytes(x.begin(), x.size());
}

// FIXME: make more explicit
inline
bytes
to_bytes(const sstring& x) {
    return bytes(reinterpret_cast<const int8_t*>(x.c_str()), x.size());
}

inline
bytes_view
to_bytes_view(const sstring& x) {
    return bytes_view(reinterpret_cast<const int8_t*>(x.c_str()), x.size());
}

inline
bytes
to_bytes(const utils::UUID& uuid) {
    struct {
        uint64_t msb;
        uint64_t lsb;
    } tmp = { net::hton(uint64_t(uuid.get_most_significant_bits())),
        net::hton(uint64_t(uuid.get_least_significant_bits())) };
    return bytes(reinterpret_cast<int8_t*>(&tmp), 16);
}

// This follows java.util.Comparator
// FIXME: Choose a better place than database.hh
template <typename T>
struct comparator {
    comparator() = default;
    comparator(std::function<int32_t (T& v1, T& v2)> fn)
        : _compare_fn(std::move(fn))
    { }
    int32_t compare() { return _compare_fn(); }
private:
    std::function<int32_t (T& v1, T& v2)> _compare_fn;
};

inline bool
less_unsigned(bytes_view v1, bytes_view v2) {
    return compare_unsigned(v1, v2) < 0;
}

bytes_view
read_simple_bytes(bytes_view& v, size_t n) {
    if (v.size() < n) {
        throw marshal_exception();
    }
    bytes_view ret(v.begin(), n);
    v.remove_prefix(n);
    return ret;
}

template<typename T>
std::experimental::optional<T> read_simple_opt(bytes_view& v) {
    if (v.empty()) {
        return {};
    }
    if (v.size() != sizeof(T)) {
        throw marshal_exception();
    }
    auto p = v.begin();
    v.remove_prefix(sizeof(T));
    return { net::ntoh(*reinterpret_cast<const net::packed<T>*>(p)) };
}

*/
