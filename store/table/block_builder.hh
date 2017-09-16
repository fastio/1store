#pragma once
#include <vector>
#include <stdint.h>
#include "seastarx.hh"
#include "utils/bytes.hh"
#include "store/comparator.hh"
namespace store {

struct block_options {
    explicit block_options(const comparator& c = default_bytewise_comparator()) : _block_restart_interval (1024), _comparator(c) {}
    uint32_t _block_restart_interval = 64;
    const comparator& _comparator;
};

class block_builder {
 public:
  explicit block_builder(const block_options& options);

  // Reset the contents as if the BlockBuilder was just constructed.
  void reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void add(const bytes& key, const bytes& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  const bytes& finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t current_size_estimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return _buffer.empty();
  }

  block_builder(const block_builder&) = delete;
  void operator=(const block_builder&) = delete;

 private:
  const block_options   _options;
  bytes                 _buffer;      // Destination buffer
  std::vector<uint32_t> _restarts;    // Restart points
  uint32_t              _counter;     // Number of entries emitted since restart
  bool                  _finished;    // Has Finish() been called?
  bytes                 _last_key;
};

}
