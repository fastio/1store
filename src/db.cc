#include "db.hh"
#include "redis_commands.hh"
namespace redis {

static constexpr double default_slab_growth_factor = 1.25;
static constexpr uint64_t default_slab_page_size = 1UL*MB;
static constexpr uint64_t default_per_cpu_slab_size = 512UL; // zero means reclaimer is enabled.
__thread slab_allocator<item>* _slab;
__thread redis_commands* _redis_commands_ptr;
db::db() : _store(new dict())
{
  _slab = new slab_allocator<item>(default_slab_growth_factor, default_per_cpu_slab_size, default_slab_page_size,
      [this](item& item_ref) {  });
  _redis_commands_ptr = new redis_commands();
}
db::~db()
{
  if (_store != nullptr) {
    delete _store;
  }
}

}
