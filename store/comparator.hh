#pragma once
#include "utils/bytes.hh"

namespace store {

class comparator {
public:
    comparator() {}
    ~comparator() {}

    virtual int compare(const bytes_view& a, const bytes_view& b) const = 0;
    virtual int compare(const bytes& a, const bytes_view& b) const = 0;
    virtual int compare(const bytes_view& a, const bytes& b) const = 0;
    virtual int compare(const bytes& a, const bytes& b) const = 0;
};

extern const comparator& default_bytewise_comparator();

}
