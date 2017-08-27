#!/usr/bin/python3
#
#
import os, os.path, textwrap, argparse, sys, shlex, subprocess, tempfile, re
from distutils.spawn import find_executable

configure_args = str.join(' ', [shlex.quote(x) for x in sys.argv[1:]])

for line in open('/etc/os-release'):
    key, _, value = line.partition('=')
    value = value.strip().strip('"')
    if key == 'ID':
        os_ids = [value]
    if key == 'ID_LIKE':
        os_ids += value.split(' ')

# distribution "internationalization", converting package names.
# Fedora name is key, values is distro -> package name dict.
i18n_xlat = {
    'boost-devel': {
        'debian': 'libboost-dev',
        'ubuntu': 'libboost-dev (libboost1.55-dev on 14.04)',
    },
}

def pkgname(name):
    if name in i18n_xlat:
        dict = i18n_xlat[name]
        for id in os_ids:
            if id in dict:
                return dict[id]
    return name

def get_flags():
    with open('/proc/cpuinfo') as f:
        for line in f:
            if line.strip():
                if line.rstrip('\n').startswith('flags'):
                    return re.sub(r'^flags\s+: ', '', line).split()

def add_tristate(arg_parser, name, dest, help):
    arg_parser.add_argument('--enable-' + name, dest = dest, action = 'store_true', default = None,
                            help = 'Enable ' + help)
    arg_parser.add_argument('--disable-' + name, dest = dest, action = 'store_false', default = None,
                            help = 'Disable ' + help)

def apply_tristate(var, test, note, missing):
    if (var is None) or var:
        if test():
            return True
        elif var == True:
            print(missing)
            sys.exit(1)
        else:
            print(note)
            return False
    return False

def have_pkg(package):
    return subprocess.call(['pkg-config', package]) == 0

def pkg_config(option, package):
    output = subprocess.check_output(['pkg-config', option, package])
    return output.decode('utf-8').strip()

def try_compile(compiler, source = '', flags = []):
    with tempfile.NamedTemporaryFile() as sfile:
        sfile.file.write(bytes(source, 'utf-8'))
        sfile.file.flush()
        return subprocess.call([compiler, '-x', 'c++', '-o', '/dev/null', '-c', sfile.name] + flags,
                               stdout = subprocess.DEVNULL,
                               stderr = subprocess.DEVNULL) == 0

def warning_supported(warning, compiler):
    # gcc ignores -Wno-x even if it is not supported
    adjusted = re.sub('^-Wno-', '-W', warning)
    return try_compile(flags = ['-Werror', adjusted], compiler = compiler)

def debug_flag(compiler):
    src_with_auto = textwrap.dedent('''\
        template <typename T>
        struct x { auto f() {} };

        x<int> a;
        ''')
    if try_compile(source = src_with_auto, flags = ['-g', '-std=gnu++1y'], compiler = compiler):
        return '-g'
    else:
        print('Note: debug information disabled; upgrade your compiler')
        return ''

def maybe_static(flag, libs):
    if flag and not args.static:
        libs = '-Wl,-Bstatic {} -Wl,-Bdynamic'.format(libs)
    return libs

class Thrift(object):
    def __init__(self, source, service):
        self.source = source
        self.service = service
    def generated(self, gen_dir):
        basename = os.path.splitext(os.path.basename(self.source))[0]
        files = [basename + '_' + ext
                 for ext in ['types.cpp', 'types.h', 'constants.cpp', 'constants.h']]
        files += [self.service + ext
                  for ext in ['.cpp', '.h']]
        return [os.path.join(gen_dir, file) for file in files]
    def headers(self, gen_dir):
        return [x for x in self.generated(gen_dir) if x.endswith('.h')]
    def sources(self, gen_dir):
        return [x for x in self.generated(gen_dir) if x.endswith('.cpp')]
    def objects(self, gen_dir):
        return [x.replace('.cpp', '.o') for x in self.sources(gen_dir)]
    def endswith(self, end):
        return self.source.endswith(end)

class Antlr3Grammar(object):
    def __init__(self, source):
        self.source = source
    def generated(self, gen_dir):
        basename = os.path.splitext(self.source)[0]
        files = [basename + ext
                 for ext in ['Lexer.cpp', 'Lexer.hpp', 'Parser.cpp', 'Parser.hpp']]
        return [os.path.join(gen_dir, file) for file in files]
    def headers(self, gen_dir):
        return [x for x in self.generated(gen_dir) if x.endswith('.hpp')]
    def sources(self, gen_dir):
        return [x for x in self.generated(gen_dir) if x.endswith('.cpp')]
    def objects(self, gen_dir):
        return [x.replace('.cpp', '.o') for x in self.sources(gen_dir)]
    def endswith(self, end):
        return self.source.endswith(end)

modes = {
    'debug': {
        'sanitize': '-fsanitize=address -fsanitize=leak -fsanitize=undefined',
        'sanitize_libs': '-lasan -lubsan',
        'opt': '-O0 -DDEBUG -DDEBUG_SHARED_PTR -DDEFAULT_ALLOCATOR',
        'libs': '',
    },
    'release': {
        'sanitize': '',
        'sanitize_libs': '',
        'opt': '-O2',
        'libs': '',
    },
}

scylla_tests = [
    'scylla/tests/mutation_test',
    'scylla/tests/streamed_mutation_test',
    'scylla/tests/schema_registry_test',
    'scylla/tests/canonical_mutation_test',
    'scylla/tests/range_test',
    'scylla/tests/types_test',
    'scylla/tests/keys_test',
    'scylla/tests/partitioner_test',
    'scylla/tests/frozen_mutation_test',
    'scylla/tests/perf/perf_mutation',
    'scylla/tests/lsa_async_eviction_test',
    'scylla/tests/lsa_sync_eviction_test',
    'scylla/tests/row_cache_alloc_stress',
    'scylla/tests/perf_row_cache_update',
    'scylla/tests/perf/perf_hash',
    'scylla/tests/perf/perf_cql_parser',
    'scylla/tests/perf/perf_simple_query',
    'scylla/tests/perf/perf_fast_forward',
    'scylla/tests/cache_streamed_mutation_test',
    'scylla/tests/row_cache_stress_test',
    'scylla/tests/memory_footprint',
    'scylla/tests/perf/perf_sstable',
    'scylla/tests/cql_query_test',
    'scylla/tests/storage_proxy_test',
    'scylla/tests/schema_change_test',
    'scylla/tests/mutation_reader_test',
    'scylla/tests/mutation_query_test',
    'scylla/tests/row_cache_test',
    'scylla/tests/test-serialization',
    'scylla/tests/sstable_test',
    'scylla/tests/sstable_mutation_test',
    'scylla/tests/sstable_resharding_test',
    'scylla/tests/memtable_test',
    'scylla/tests/commitlog_test',
    'scylla/tests/cartesian_product_test',
    'scylla/tests/hash_test',
    'scylla/tests/map_difference_test',
    'scylla/tests/message',
    'scylla/tests/gossip',
    'scylla/tests/gossip_test',
    'scylla/tests/compound_test',
    'scylla/tests/config_test',
    'scylla/tests/gossiping_property_file_snitch_test',
    'scylla/tests/ec2_snitch_test',
    'scylla/tests/snitch_reset_test',
    'scylla/tests/network_topology_strategy_test',
    'scylla/tests/query_processor_test',
    'scylla/tests/batchlog_manager_test',
    'scylla/tests/bytes_ostream_test',
    'scylla/tests/UUID_test',
    'scylla/tests/murmur_hash_test',
    'scylla/tests/allocation_strategy_test',
    'scylla/tests/logalloc_test',
    'scylla/tests/log_histogram_test',
    'scylla/tests/managed_vector_test',
    'scylla/tests/crc_test',
    'scylla/tests/flush_queue_test',
    'scylla/tests/dynamic_bitset_test',
    'scylla/tests/auth_test',
    'scylla/tests/idl_test',
    'scylla/tests/range_tombstone_list_test',
    'scylla/tests/anchorless_list_test',
    'scylla/tests/database_test',
    'scylla/tests/nonwrapping_range_test',
    'scylla/tests/input_stream_test',
    'scylla/tests/sstable_atomic_deletion_test',
    'scylla/tests/virtual_reader_test',
    'scylla/tests/view_schema_test',
    'scylla/tests/counter_test',
    'scylla/tests/cell_locker_test',
]

apps = [
    'pedis',
    ]

tests = scylla_tests

other = [
    'iotune',
    ]

all_artifacts = apps + tests + other

arg_parser = argparse.ArgumentParser('Configure Pedis')
arg_parser.add_argument('--static', dest = 'static', action = 'store_const', default = '',
                        const = '-static',
                        help = 'Static link (useful for running on hosts outside the build environment')
arg_parser.add_argument('--pie', dest = 'pie', action = 'store_true',
                        help = 'Build position-independent executable (PIE)')
arg_parser.add_argument('--so', dest = 'so', action = 'store_true',
                        help = 'Build shared object (SO) instead of executable')
arg_parser.add_argument('--mode', action='store', choices=list(modes.keys()) + ['all'], default='all')
arg_parser.add_argument('--with', dest='artifacts', action='append', choices=all_artifacts, default=[])
arg_parser.add_argument('--cflags', action = 'store', dest = 'user_cflags', default = '',
                        help = 'Extra flags for the C++ compiler')
arg_parser.add_argument('--ldflags', action = 'store', dest = 'user_ldflags', default = '',
                        help = 'Extra flags for the linker')
arg_parser.add_argument('--compiler', action = 'store', dest = 'cxx', default = 'g++',
                        help = 'C++ compiler path')
arg_parser.add_argument('--c-compiler', action='store', dest='cc', default='gcc',
                        help='C compiler path')
arg_parser.add_argument('--with-osv', action = 'store', dest = 'with_osv', default = '',
                        help = 'Shortcut for compile for OSv')
arg_parser.add_argument('--enable-dpdk', action = 'store_true', dest = 'dpdk', default = False,
                        help = 'Enable dpdk (from seastar dpdk sources)')
arg_parser.add_argument('--dpdk-target', action = 'store', dest = 'dpdk_target', default = '',
                        help = 'Path to DPDK SDK target location (e.g. <DPDK SDK dir>/x86_64-native-linuxapp-gcc)')
arg_parser.add_argument('--debuginfo', action = 'store', dest = 'debuginfo', type = int, default = 1,
                        help = 'Enable(1)/disable(0)compiler debug information generation')
arg_parser.add_argument('--static-stdc++', dest = 'staticcxx', action = 'store_true',
			help = 'Link libgcc and libstdc++ statically')
arg_parser.add_argument('--static-thrift', dest = 'staticthrift', action = 'store_true',
            help = 'Link libthrift statically')
arg_parser.add_argument('--static-boost', dest = 'staticboost', action = 'store_true',
            help = 'Link boost statically')
arg_parser.add_argument('--tests-debuginfo', action = 'store', dest = 'tests_debuginfo', type = int, default = 0,
                        help = 'Enable(1)/disable(0)compiler debug information generation for tests')
arg_parser.add_argument('--python', action = 'store', dest = 'python', default = 'python3',
                        help = 'Python3 path')
add_tristate(arg_parser, name = 'hwloc', dest = 'hwloc', help = 'hwloc support')
add_tristate(arg_parser, name = 'xen', dest = 'xen', help = 'Xen support')
arg_parser.add_argument('--enable-gcc6-concepts', dest='gcc6_concepts', action='store_true', default=False,
                        help='enable experimental support for C++ Concepts as implemented in GCC 6')
args = arg_parser.parse_args()

defines = []

extra_cxxflags = {}

cassandra_interface = Thrift(source = 'scylla/interface/cassandra.thrift', service = 'Cassandra')

scylla_core = (['scylla/database.cc',
                 'scylla/schema.cc',
                 'scylla/frozen_schema.cc',
                 'scylla/schema_registry.cc',
                 'scylla/bytes.cc',
                 'scylla/mutation.cc',
                 'scylla/streamed_mutation.cc',
                 'scylla/partition_version.cc',
                 'scylla/row_cache.cc',
                 'scylla/canonical_mutation.cc',
                 'scylla/frozen_mutation.cc',
                 'scylla/memtable.cc',
                 'scylla/schema_mutations.cc',
                 'scylla/release.cc',
                 'scylla/supervisor.cc',
                 'scylla/utils/logalloc.cc',
                 'scylla/utils/large_bitset.cc',
                 'scylla/mutation_partition.cc',
                 'scylla/mutation_partition_view.cc',
                 'scylla/mutation_partition_serializer.cc',
                 'scylla/mutation_reader.cc',
                 'scylla/mutation_query.cc',
                 'scylla/keys.cc',
                 'scylla/counters.cc',
                 'scylla/sstables/sstables.cc',
                 'scylla/sstables/compress.cc',
                 'scylla/sstables/row.cc',
                 'scylla/sstables/partition.cc',
                 'scylla/sstables/filter.cc',
                 'scylla/sstables/compaction.cc',
                 'scylla/sstables/compaction_strategy.cc',
                 'scylla/sstables/compaction_manager.cc',
                 'scylla/sstables/atomic_deletion.cc',
                 'scylla/transport/event.cc',
                 'scylla/transport/event_notifier.cc',
                 'scylla/transport/server.cc',
                 'scylla/cql3/abstract_marker.cc',
                 'scylla/cql3/attributes.cc',
                 'scylla/cql3/cf_name.cc',
                 'scylla/cql3/cql3_type.cc',
                 'scylla/cql3/operation.cc',
                 'scylla/cql3/index_name.cc',
                 'scylla/cql3/keyspace_element_name.cc',
                 'scylla/cql3/lists.cc',
                 'scylla/cql3/sets.cc',
                 'scylla/cql3/maps.cc',
                 'scylla/cql3/functions/functions.cc',
                 'scylla/cql3/statements/cf_prop_defs.cc',
                 'scylla/cql3/statements/cf_statement.cc',
                 'scylla/cql3/statements/authentication_statement.cc',
                 'scylla/cql3/statements/create_keyspace_statement.cc',
                 'scylla/cql3/statements/create_table_statement.cc',
                 'scylla/cql3/statements/create_view_statement.cc',
                 'scylla/cql3/statements/create_type_statement.cc',
                 'scylla/cql3/statements/create_user_statement.cc',
                 'scylla/cql3/statements/drop_index_statement.cc',
                 'scylla/cql3/statements/drop_keyspace_statement.cc',
                 'scylla/cql3/statements/drop_table_statement.cc',
                 'scylla/cql3/statements/drop_view_statement.cc',
                 'scylla/cql3/statements/drop_type_statement.cc',
                 'scylla/cql3/statements/schema_altering_statement.cc',
                 'scylla/cql3/statements/ks_prop_defs.cc',
                 'scylla/cql3/statements/modification_statement.cc',
                 'scylla/cql3/statements/parsed_statement.cc',
                 'scylla/cql3/statements/property_definitions.cc',
                 'scylla/cql3/statements/update_statement.cc',
                 'scylla/cql3/statements/delete_statement.cc',
                 'scylla/cql3/statements/batch_statement.cc',
                 'scylla/cql3/statements/select_statement.cc',
                 'scylla/cql3/statements/use_statement.cc',
                 'scylla/cql3/statements/index_prop_defs.cc',
                 'scylla/cql3/statements/index_target.cc',
                 'scylla/cql3/statements/create_index_statement.cc',
                 'scylla/cql3/statements/truncate_statement.cc',
                 'scylla/cql3/statements/alter_table_statement.cc',
                 'scylla/cql3/statements/alter_view_statement.cc',
                 'scylla/cql3/statements/alter_user_statement.cc',
                 'scylla/cql3/statements/drop_user_statement.cc',
                 'scylla/cql3/statements/list_users_statement.cc',
                 'scylla/cql3/statements/authorization_statement.cc',
                 'scylla/cql3/statements/permission_altering_statement.cc',
                 'scylla/cql3/statements/list_permissions_statement.cc',
                 'scylla/cql3/statements/grant_statement.cc',
                 'scylla/cql3/statements/revoke_statement.cc',
                 'scylla/cql3/statements/alter_type_statement.cc',
                 'scylla/cql3/statements/alter_keyspace_statement.cc',
                 'scylla/cql3/update_parameters.cc',
                 'scylla/cql3/ut_name.cc',
                 'scylla/cql3/user_options.cc',
                 'scylla/thrift/handler.cc',
                 'scylla/thrift/server.cc',
                 'scylla/thrift/thrift_validation.cc',
                 'scylla/utils/runtime.cc',
                 'scylla/utils/murmur_hash.cc',
                 'scylla/utils/uuid.cc',
                 'scylla/utils/big_decimal.cc',
                 'scylla/types.cc',
                 'scylla/validation.cc',
                 'scylla/service/priority_manager.cc',
                 'scylla/service/migration_manager.cc',
                 'scylla/service/storage_proxy.cc',
                 'scylla/cql3/operator.cc',
                 'scylla/cql3/relation.cc',
                 'scylla/cql3/column_identifier.cc',
                 'scylla/cql3/column_specification.cc',
                 'scylla/cql3/constants.cc',
                 'scylla/cql3/query_processor.cc',
                 'scylla/cql3/query_options.cc',
                 'scylla/cql3/single_column_relation.cc',
                 'scylla/cql3/token_relation.cc',
                 'scylla/cql3/column_condition.cc',
                 'scylla/cql3/user_types.cc',
                 'scylla/cql3/untyped_result_set.cc',
                 'scylla/cql3/selection/abstract_function_selector.cc',
                 'scylla/cql3/selection/simple_selector.cc',
                 'scylla/cql3/selection/selectable.cc',
                 'scylla/cql3/selection/selector_factories.cc',
                 'scylla/cql3/selection/selection.cc',
                 'scylla/cql3/selection/selector.cc',
                 'scylla/cql3/restrictions/statement_restrictions.cc',
                 'scylla/cql3/result_set.cc',
                 'scylla/cql3/variable_specifications.cc',
                 'scylla/db/consistency_level.cc',
                 'scylla/db/system_keyspace.cc',
                 'scylla/db/schema_tables.cc',
                 'scylla/db/cql_type_parser.cc',
                 'scylla/db/legacy_schema_migrator.cc',
                 'scylla/db/commitlog/commitlog.cc',
                 'scylla/db/commitlog/commitlog_replayer.cc',
                 'scylla/db/commitlog/commitlog_entry.cc',
                 'scylla/db/config.cc',
                 'scylla/db/heat_load_balance.cc',
                 'scylla/db/index/secondary_index.cc',
                 'scylla/db/marshal/type_parser.cc',
                 'scylla/db/batchlog_manager.cc',
                 'scylla/db/view/view.cc',
                 'scylla/index/secondary_index_manager.cc',
                 'scylla/io/io.cc',
                 'scylla/utils/utils.cc',
                 'scylla/utils/UUID_gen.cc',
                 'scylla/utils/i_filter.cc',
                 'scylla/utils/bloom_filter.cc',
                 'scylla/utils/bloom_calculations.cc',
                 'scylla/utils/rate_limiter.cc',
                 'scylla/utils/file_lock.cc',
                 'scylla/utils/dynamic_bitset.cc',
                 'scylla/utils/managed_bytes.cc',
                 'scylla/utils/exceptions.cc',
                 'scylla/gms/version_generator.cc',
                 'scylla/gms/versioned_value.cc',
                 'scylla/gms/gossiper.cc',
                 'scylla/gms/failure_detector.cc',
                 'scylla/gms/gossip_digest_syn.cc',
                 'scylla/gms/gossip_digest_ack.cc',
                 'scylla/gms/gossip_digest_ack2.cc',
                 'scylla/gms/endpoint_state.cc',
                 'scylla/gms/application_state.cc',
                 'scylla/gms/inet_address.cc',
                 'scylla/dht/i_partitioner.cc',
                 'scylla/dht/murmur3_partitioner.cc',
                 'scylla/dht/byte_ordered_partitioner.cc',
                 'scylla/dht/random_partitioner.cc',
                 'scylla/dht/boot_strapper.cc',
                 'scylla/dht/range_streamer.cc',
                 'scylla/unimplemented.cc',
                 'scylla/query.cc',
                 'scylla/query-result-set.cc',
                 'scylla/locator/abstract_replication_strategy.cc',
                 'scylla/locator/simple_strategy.cc',
                 'scylla/locator/local_strategy.cc',
                 'scylla/locator/network_topology_strategy.cc',
                 'scylla/locator/everywhere_replication_strategy.cc',
                 'scylla/locator/token_metadata.cc',
                 'scylla/locator/locator.cc',
                 'scylla/locator/snitch_base.cc',
                 'scylla/locator/simple_snitch.cc',
                 'scylla/locator/rack_inferring_snitch.cc',
                 'scylla/locator/gossiping_property_file_snitch.cc',
                 'scylla/locator/production_snitch_base.cc',
                 'scylla/locator/ec2_snitch.cc',
                 'scylla/locator/ec2_multi_region_snitch.cc',
                 'scylla/message/messaging_service.cc',
                 'scylla/service/client_state.cc',
                 'scylla/service/migration_task.cc',
                 'scylla/service/storage_service.cc',
                 'scylla/service/misc_services.cc',
                 'scylla/service/pager/paging_state.cc',
                 'scylla/service/pager/query_pagers.cc',
                 'scylla/streaming/stream_task.cc',
                 'scylla/streaming/stream_session.cc',
                 'scylla/streaming/stream_request.cc',
                 'scylla/streaming/stream_summary.cc',
                 'scylla/streaming/stream_transfer_task.cc',
                 'scylla/streaming/stream_receive_task.cc',
                 'scylla/streaming/stream_plan.cc',
                 'scylla/streaming/progress_info.cc',
                 'scylla/streaming/session_info.cc',
                 'scylla/streaming/stream_coordinator.cc',
                 'scylla/streaming/stream_manager.cc',
                 'scylla/streaming/stream_result_future.cc',
                 'scylla/streaming/stream_session_state.cc',
                 'scylla/clocks-impl.cc',
                 'scylla/partition_slice_builder.cc',
                 'scylla/init.cc',
                 'scylla/lister.cc',
                 'scylla/repair/repair.cc',
                 'scylla/exceptions/exceptions.cc',
                 'scylla/auth/auth.cc',
                 'scylla/auth/authenticated_user.cc',
                 'scylla/auth/authenticator.cc',
                 'scylla/auth/authorizer.cc',
                 'scylla/auth/default_authorizer.cc',
                 'scylla/auth/data_resource.cc',
                 'scylla/auth/password_authenticator.cc',
                 'scylla/auth/permission.cc',
                 'scylla/tracing/tracing.cc',
                 'scylla/tracing/trace_keyspace_helper.cc',
                 'scylla/tracing/trace_state.cc',
                 'scylla/range_tombstone.cc',
                 'scylla/range_tombstone_list.cc',
                 'scylla/disk-error-handler.cc',
                 'redis_protocol_parser.rl',
                 'redis_protocol.cc',
                 'request_wrapper.cc',
                 'cache.cc',
                 'db.cc',
                 'hll.cc',
                 'geo.cc',
                 'list_lsa.cc',
                 'redis_service.cc',
                 'reply_builder.cc',
                 'request_wrapper.cc',
                 'sset_lsa.cc',
                 'bits_operation.cc',
                 'redis_server.cc',
                 'redis_storage_proxy.cc',
                 ]
                + [Antlr3Grammar('scylla/cql3/Cql.g')]
                + [Thrift('scylla/interface/cassandra.thrift', 'Cassandra')]
                )
api = ['scylla/api/api.cc',
       'scylla/api/api-doc/storage_service.json',
       'scylla/api/api-doc/lsa.json',
       'scylla/api/storage_service.cc',
       'scylla/api/api-doc/commitlog.json',
       'scylla/api/commitlog.cc',
       'scylla/api/api-doc/gossiper.json',
       'scylla/api/gossiper.cc',
       'scylla/api/api-doc/failure_detector.json',
       'scylla/api/failure_detector.cc',
       'scylla/api/api-doc/column_family.json',
       'scylla/api/column_family.cc',
       'scylla/api/messaging_service.cc',
       'scylla/api/api-doc/messaging_service.json',
       'scylla/api/api-doc/storage_proxy.json',
       'scylla/api/storage_proxy.cc',
       'scylla/api/api-doc/cache_service.json',
       'scylla/api/cache_service.cc',
       'scylla/api/api-doc/collectd.json',
       'scylla/api/collectd.cc',
       'scylla/api/api-doc/endpoint_snitch_info.json',
       'scylla/api/endpoint_snitch.cc',
       'scylla/api/api-doc/compaction_manager.json',
       'scylla/api/compaction_manager.cc',
       'scylla/api/api-doc/hinted_handoff.json',
       'scylla/api/hinted_handoff.cc',
       'scylla/api/api-doc/utils.json',
       'scylla/api/lsa.cc',
       'scylla/api/api-doc/stream_manager.json',
       'scylla/api/stream_manager.cc',
       'scylla/api/api-doc/system.json',
       'scylla/api/system.cc'
       ]

idls = ['scylla/idl/gossip_digest.idl.hh',
        'scylla/idl/uuid.idl.hh',
        'scylla/idl/range.idl.hh',
        'scylla/idl/keys.idl.hh',
        'scylla/idl/read_command.idl.hh',
        'scylla/idl/token.idl.hh',
        'scylla/idl/ring_position.idl.hh',
        'scylla/idl/result.idl.hh',
        'scylla/idl/frozen_mutation.idl.hh',
        'scylla/idl/reconcilable_result.idl.hh',
        'scylla/idl/streaming.idl.hh',
        'scylla/idl/paging_state.idl.hh',
        'scylla/idl/frozen_schema.idl.hh',
        'scylla/idl/partition_checksum.idl.hh',
        'scylla/idl/replay_position.idl.hh',
        'scylla/idl/truncation_record.idl.hh',
        'scylla/idl/mutation.idl.hh',
        'scylla/idl/query.idl.hh',
        'scylla/idl/idl_test.idl.hh',
        'scylla/idl/commitlog.idl.hh',
        'scylla/idl/tracing.idl.hh',
        'scylla/idl/consistency_level.idl.hh',
        'scylla/idl/cache_temperature.idl.hh',
        ]

scylla_tests_dependencies = scylla_core + api + idls + [
    'scylla/tests/cql_test_env.cc',
    'scylla/tests/cql_assertions.cc',
    'scylla/tests/result_set_assertions.cc',
    'scylla/tests/mutation_source_test.cc',
]

scylla_tests_seastar_deps = [
    'seastar/tests/test-utils.cc',
    'seastar/tests/test_runner.cc',
]

deps = {
    'pedis': idls + ['main.cc'] + scylla_core + api,
}

pure_boost_tests = set([
    'scylla/tests/partitioner_test',
    'scylla/tests/map_difference_test',
    'scylla/tests/keys_test',
    'scylla/tests/compound_test',
    'scylla/tests/range_tombstone_list_test',
    'scylla/tests/anchorless_list_test',
    'scylla/tests/nonwrapping_range_test',
    'scylla/tests/test-serialization',
    'scylla/tests/range_test',
    'scylla/tests/crc_test',
    'scylla/tests/managed_vector_test',
    'scylla/tests/dynamic_bitset_test',
    'scylla/tests/idl_test',
    'scylla/tests/cartesian_product_test',
])

tests_not_using_seastar_test_framework = set([
    'scylla/tests/perf/perf_mutation',
    'scylla/tests/lsa_async_eviction_test',
    'scylla/tests/lsa_sync_eviction_test',
    'scylla/tests/row_cache_alloc_stress',
    'scylla/tests/perf_row_cache_update',
    'scylla/tests/perf/perf_hash',
    'scylla/tests/perf/perf_cql_parser',
    'scylla/tests/message',
    'scylla/tests/perf/perf_simple_query',
    'scylla/tests/perf/perf_fast_forward',
    'scylla/tests/row_cache_stress_test',
    'scylla/tests/memory_footprint',
    'scylla/tests/gossip',
    'scylla/tests/perf/perf_sstable',
]) | pure_boost_tests

for t in tests_not_using_seastar_test_framework:
    if not t in scylla_tests:
        raise Exception("Test %s not found in scylla_tests" % (t))

for t in scylla_tests:
    deps[t] = [t + '.cc']
    if t not in tests_not_using_seastar_test_framework:
        deps[t] += scylla_tests_dependencies
        deps[t] += scylla_tests_seastar_deps
    else:
        deps[t] += scylla_core + api + idls + ['scylla/tests/cql_test_env.cc']

deps['scylla/tests/sstable_test'] += ['scylla/tests/sstable_datafile_test.cc']

deps['scylla/tests/bytes_ostream_test'] = ['scylla/tests/bytes_ostream_test.cc', 'scylla/utils/managed_bytes.cc', 'scylla/utils/logalloc.cc', 'scylla/utils/dynamic_bitset.cc']
deps['scylla/tests/input_stream_test'] = ['scylla/tests/input_stream_test.cc']
deps['scylla/tests/UUID_test'] = ['scylla/utils/UUID_gen.cc', 'scylla/tests/UUID_test.cc', 'scylla/utils/uuid.cc', 'scylla/utils/managed_bytes.cc', 'scylla/utils/logalloc.cc', 'scylla/utils/dynamic_bitset.cc']
deps['scylla/tests/murmur_hash_test'] = ['scylla/bytes.cc', 'scylla/utils/murmur_hash.cc', 'scylla/tests/murmur_hash_test.cc']
deps['scylla/tests/allocation_strategy_test'] = ['scylla/tests/allocation_strategy_test.cc', 'scylla/utils/logalloc.cc', 'scylla/utils/dynamic_bitset.cc']
deps['scylla/tests/log_histogram_test'] = ['scylla/tests/log_histogram_test.cc']
deps['scylla/tests/anchorless_list_test'] = ['scylla/tests/anchorless_list_test.cc']

warnings = [
    '-Wno-mismatched-tags',  # clang-only
    '-Wno-maybe-uninitialized', # false positives on gcc 5
    '-Wno-tautological-compare',
    '-Wno-parentheses-equality',
    '-Wno-c++11-narrowing',
    '-Wno-c++1z-extensions',
    '-Wno-sometimes-uninitialized',
    '-Wno-return-stack-address',
    '-Wno-missing-braces',
    '-Wno-unused-lambda-capture',
    ]

warnings = [w
            for w in warnings
            if warning_supported(warning = w, compiler = args.cxx)]

warnings = ' '.join(warnings + ['-Wno-error=deprecated-declarations'])

dbgflag = debug_flag(args.cxx) if args.debuginfo else ''
tests_link_rule = 'link' if args.tests_debuginfo else 'link_stripped'

if args.so:
    args.pie = '-shared'
    args.fpie = '-fpic'
elif args.pie:
    args.pie = '-pie'
    args.fpie = '-fpie'
else:
    args.pie = ''
    args.fpie = ''

# a list element means a list of alternative packages to consider
# the first element becomes the HAVE_pkg define
# a string element is a package name with no alternatives
optional_packages = [['libsystemd', 'libsystemd-daemon']]
pkgs = []

def setup_first_pkg_of_list(pkglist):
    # The HAVE_pkg symbol is taken from the first alternative
    upkg = pkglist[0].upper().replace('-', '_')
    for pkg in pkglist:
        if have_pkg(pkg):
            pkgs.append(pkg)
            defines.append('HAVE_{}=1'.format(upkg))
            return True
    return False

for pkglist in optional_packages:
    if isinstance(pkglist, str):
        pkglist = [pkglist]
    if not setup_first_pkg_of_list(pkglist):
        if len(pkglist) == 1:
            print('Missing optional package {pkglist[0]}'.format(**locals()))
        else:
            alternatives = ':'.join(pkglist[1:])
            print('Missing optional package {pkglist[0]} (or alteratives {alternatives})'.format(**locals()))

if not try_compile(compiler=args.cxx, source='#include <boost/version.hpp>'):
    print('Boost not installed.  Please install {}.'.format(pkgname("boost-devel")))
    sys.exit(1)

if not try_compile(compiler=args.cxx, source='''\
        #include <boost/version.hpp>
        #if BOOST_VERSION < 105500
        #error Boost version too low
        #endif
        '''):
    print('Installed boost version too old.  Please update {}.'.format(pkgname("boost-devel")))
    sys.exit(1)

defines = ' '.join(['-D' + d for d in defines])

globals().update(vars(args))

total_memory = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
link_pool_depth = max(int(total_memory / 7e9), 1)

build_modes = modes if args.mode == 'all' else [args.mode]
build_artifacts = all_artifacts if not args.artifacts else args.artifacts

status = subprocess.call("./scylla/SCYLLA-VERSION-GEN")
if status != 0:
    print('Version file generation failed')
    sys.exit(1)

file = open('build/SCYLLA-VERSION-FILE', 'r')
scylla_version = file.read().strip()
file = open('build/SCYLLA-RELEASE-FILE', 'r')
scylla_release = file.read().strip()

extra_cxxflags["scylla/release.cc"] = "-DSCYLLA_VERSION=\"\\\"" + scylla_version + "\\\"\" -DSCYLLA_RELEASE=\"\\\"" + scylla_release + "\\\"\""

seastar_flags = []
if args.dpdk:
    # fake dependencies on dpdk, so that it is built before anything else
    seastar_flags += ['--enable-dpdk']
elif args.dpdk_target:
    seastar_flags += ['--dpdk-target', args.dpdk_target]
if args.staticcxx:
    seastar_flags += ['--static-stdc++']
if args.staticboost:
    seastar_flags += ['--static-boost']
if args.gcc6_concepts:
    seastar_flags += ['--enable-gcc6-concepts']

seastar_cflags = args.user_cflags + " -march=nehalem"
seastar_flags += ['--compiler', args.cxx, '--c-compiler', args.cc, '--cflags=%s' % (seastar_cflags)]

status = subprocess.call([python, './configure.py'] + seastar_flags, cwd = 'seastar')

if status != 0:
    print('Seastar configuration failed')
    sys.exit(1)


pc = { mode : 'build/{}/seastar.pc'.format(mode) for mode in build_modes }
ninja = find_executable('ninja') or find_executable('ninja-build')
if not ninja:
    print('Ninja executable (ninja or ninja-build) not found on PATH\n')
    sys.exit(1)
status = subprocess.call([ninja] + list(pc.values()), cwd = 'seastar')
if status:
    print('Failed to generate {}\n'.format(pc))
    sys.exit(1)

for mode in build_modes:
    cfg =  dict([line.strip().split(': ', 1)
                 for line in open('seastar/' + pc[mode])
                 if ': ' in line])
    if args.staticcxx:
        cfg['Libs'] = cfg['Libs'].replace('-lstdc++ ', '')
    modes[mode]['seastar_cflags'] = cfg['Cflags']
    modes[mode]['seastar_libs'] = cfg['Libs']

seastar_deps = 'practically_anything_can_change_so_lets_run_it_every_time_and_restat.'

args.user_cflags += " " + pkg_config("--cflags", "jsoncpp")
libs = ' '.join(['-lyaml-cpp', '-llz4', '-lz', '-lsnappy', pkg_config("--libs", "jsoncpp"),
                 maybe_static(args.staticboost, '-lboost_filesystem'), ' -lcryptopp -lcrypt',
                 maybe_static(args.staticboost, '-lboost_date_time'),
                ])

if not args.staticboost:
    args.user_cflags += ' -DBOOST_TEST_DYN_LINK'

for pkg in pkgs:
    args.user_cflags += ' ' + pkg_config('--cflags', pkg)
    libs += ' ' + pkg_config('--libs', pkg)
user_cflags = args.user_cflags
user_ldflags = args.user_ldflags
if args.staticcxx:
    user_ldflags += " -static-libgcc -static-libstdc++"
if args.staticthrift:
    thrift_libs = "-Wl,-Bstatic -lthrift -Wl,-Bdynamic"
else:
    thrift_libs = "-lthrift"

outdir = 'build'
buildfile = 'build.ninja'

os.makedirs(outdir, exist_ok = True)
do_sanitize = True
if args.static:
    do_sanitize = False
with open(buildfile, 'w') as f:
    f.write(textwrap.dedent('''\
        configure_args = {configure_args}
        builddir = {outdir}
        cxx = {cxx}
        cxxflags = {user_cflags} {warnings} {defines}
        ldflags = {user_ldflags}
        libs = {libs}
        pool link_pool
            depth = {link_pool_depth}
        pool seastar_pool
            depth = 1
        rule ragel
            command = ragel -G2 -o $out $in
            description = RAGEL $out
        rule gen
            command = echo -e $text > $out
            description = GEN $out
        rule swagger
            command = seastar/json/json2code.py -f $in -o $out
            description = SWAGGER $out
        rule serializer
            command = {python} ./idl-compiler.py --ns ser -f $in -o $out
            description = IDL compiler $out
        rule ninja
            command = {ninja} -C $subdir $target
            restat = 1
            description = NINJA $out
        rule copy
            command = cp $in $out
            description = COPY $out
        ''').format(**globals()))
    for mode in build_modes:
        modeval = modes[mode]
        f.write(textwrap.dedent('''\
            cxxflags_{mode} = -I. -I./scylla -I $builddir/{mode}/gen -I $builddir/{mode}/gen/scylla -I seastar -I seastar/build/{mode}/gen
            rule cxx.{mode}
              command = $cxx -MD -MT $out -MF $out.d {seastar_cflags} $cxxflags $cxxflags_{mode} -c -o $out $in
              description = CXX $out
              depfile = $out.d
            rule link.{mode}
              command = $cxx  $cxxflags_{mode} {sanitize_libs} $ldflags {seastar_libs} -o $out $in $libs $libs_{mode}
              description = LINK $out
              pool = link_pool
            rule link_stripped.{mode}
              command = $cxx  $cxxflags_{mode} -s {sanitize_libs} $ldflags {seastar_libs} -o $out $in $libs $libs_{mode}
              description = LINK (stripped) $out
              pool = link_pool
            rule ar.{mode}
              command = rm -f $out; ar cr $out $in; ranlib $out
              description = AR $out
            rule thrift.{mode}
                command = thrift -gen cpp:cob_style -out $builddir/{mode}/gen $in
                description = THRIFT $in
            rule antlr3.{mode}
                command = sed -e '/^#if 0/,/^#endif/d' $in > $builddir/{mode}/gen/$in && antlr3 $builddir/{mode}/gen/$in && sed -i 's/^\\( *\)\\(ImplTraits::CommonTokenType\\* [a-zA-Z0-9_]* = NULL;\\)$$/\\1const \\2/' build/{mode}/gen/${{stem}}Parser.cpp
                description = ANTLR3 $in
            ''').format(mode = mode, **modeval))
        f.write('build {mode}: phony {artifacts}\n'.format(mode = mode,
            artifacts = str.join(' ', ('$builddir/' + mode + '/' + x for x in build_artifacts))))
        compiles = {}
        ragels = {}
        swaggers = {}
        serializers = {}
        thrifts = set()
        antlr3_grammars = set()
        for binary in build_artifacts:
            if binary in other:
                continue
            srcs = deps[binary]
            objs = ['$builddir/' + mode + '/' + src.replace('.cc', '.o')
                    for src in srcs
                    if src.endswith('.cc')]
            has_thrift = False
            for dep in deps[binary]:
                if isinstance(dep, Thrift):
                    has_thrift = True
                    objs += dep.objects('$builddir/' + mode + '/gen')
                if isinstance(dep, Antlr3Grammar):
                    objs += dep.objects('$builddir/' + mode + '/gen')
            if binary.endswith('.pc'):
                vars = modeval.copy()
                vars.update(globals())
                pc = textwrap.dedent('''\
                        Name: Seastar
                        URL: http://seastar-project.org/
                        Description: Advanced C++ framework for high-performance server applications on modern hardware.
                        Version: 1.0
                        Libs: -L{srcdir}/{builddir} -Wl,--whole-archive -lseastar -Wl,--no-whole-archive {dbgflag} -Wl,--no-as-needed {static} {pie} -fvisibility=hidden -pthread {user_ldflags} {libs} {sanitize_libs}
                        Cflags: -std=gnu++1y {dbgflag} {fpie} -Wall -Werror -fvisibility=hidden -pthread -I{srcdir} -I{srcdir}/{builddir}/gen {user_cflags} {warnings} {defines} {sanitize} {opt}
                        ''').format(builddir = 'build/' + mode, srcdir = os.getcwd(), **vars)
                f.write('build $builddir/{}/{}: gen\n  text = {}\n'.format(mode, binary, repr(pc)))
            elif binary.endswith('.a'):
                f.write('build $builddir/{}/{}: ar.{} {}\n'.format(mode, binary, mode, str.join(' ', objs)))
            else:
                if binary.startswith('tests/'):
                    local_libs = '$libs'
                    if binary not in tests_not_using_seastar_test_framework or binary in pure_boost_tests:
                        local_libs += ' ' + maybe_static(args.staticboost, '-lboost_unit_test_framework')
                    if has_thrift:
                        local_libs += ' ' + thrift_libs + ' ' + maybe_static(args.staticboost, '-lboost_system')
                    # Our code's debugging information is huge, and multiplied
                    # by many tests yields ridiculous amounts of disk space.
                    # So we strip the tests by default; The user can very
                    # quickly re-link the test unstripped by adding a "_g"
                    # to the test name, e.g., "ninja build/release/testname_g"
                    f.write('build $builddir/{}/{}: {}.{} {} {}\n'.format(mode, binary, tests_link_rule, mode, str.join(' ', objs),
                                                                                     'seastar/build/{}/libseastar.a'.format(mode)))
                    f.write('   libs = {}\n'.format(local_libs))
                    f.write('build $builddir/{}/{}_g: link.{} {} {}\n'.format(mode, binary, mode, str.join(' ', objs),
                                                                              'seastar/build/{}/libseastar.a'.format(mode)))
                    f.write('   libs = {}\n'.format(local_libs))
                else:
                    f.write('build $builddir/{}/{}: link.{} {} {}\n'.format(mode, binary, mode, str.join(' ', objs),
                                                                            'seastar/build/{}/libseastar.a'.format(mode)))
                    if has_thrift:
                        f.write('   libs =  {} {} $libs\n'.format(thrift_libs, maybe_static(args.staticboost, '-lboost_system')))
            for src in srcs:
                if src.endswith('.cc'):
                    obj = '$builddir/' + mode + '/' + src.replace('.cc', '.o')
                    compiles[obj] = src
                elif src.endswith('.rl'):
                    hh = '$builddir/' + mode + '/gen/' + src.replace('.rl', '.hh')
                    ragels[hh] = src
                elif src.endswith('.idl.hh'):
                    hh = '$builddir/' + mode + '/gen/' + src.replace('.idl.hh', '.dist.hh')
                    serializers[hh] = src
                elif src.endswith('.json'):
                    hh = '$builddir/' + mode + '/gen/' + src + '.hh'
                    swaggers[hh] = src
                elif src.endswith('.thrift'):
                    thrifts.add(src)
                elif src.endswith('.g'):
                    antlr3_grammars.add(src)
                else:
                    raise Exception('No rule for ' + src)
        for obj in compiles:
            src = compiles[obj]
            gen_headers = list(ragels.keys())
            gen_headers += ['seastar/build/{}/gen/http/request_parser.hh'.format(mode)]
            gen_headers += ['seastar/build/{}/gen/http/http_response_parser.hh'.format(mode)]
            for th in thrifts:
                gen_headers += th.headers('$builddir/{}/gen'.format(mode))
            for g in antlr3_grammars:
                gen_headers += g.headers('$builddir/{}/gen'.format(mode))
            gen_headers += list(swaggers.keys())
            gen_headers += list(serializers.keys())
            f.write('build {}: cxx.{} {} || {} \n'.format(obj, mode, src, ' '.join(gen_headers)))
            if src in extra_cxxflags:
                f.write('    cxxflags = {seastar_cflags} $cxxflags $cxxflags_{mode} {extra_cxxflags}\n'.format(mode = mode, extra_cxxflags = extra_cxxflags[src], **modeval))
        for hh in ragels:
            src = ragels[hh]
            f.write('build {}: ragel {}\n'.format(hh, src))
        for hh in swaggers:
            src = swaggers[hh]
            f.write('build {}: swagger {} | seastar/json/json2code.py\n'.format(hh,src))
        for hh in serializers:
            src = serializers[hh]
            f.write('build {}: serializer {} | idl-compiler.py\n'.format(hh,src))
        for thrift in thrifts:
            outs = ' '.join(thrift.generated('$builddir/{}/gen'.format(mode)))
            f.write('build {}: thrift.{} {}\n'.format(outs, mode, thrift.source))
            for cc in thrift.sources('$builddir/{}/gen'.format(mode)):
                obj = cc.replace('.cpp', '.o')
                f.write('build {}: cxx.{} {}\n'.format(obj, mode, cc))
        for grammar in antlr3_grammars:
            outs = ' '.join(grammar.generated('$builddir/{}/gen'.format(mode)))
            f.write('build {}: antlr3.{} {}\n  stem = {}\n'.format(outs, mode, grammar.source,
                                                                   grammar.source.rsplit('.', 1)[0]))
            for cc in grammar.sources('$builddir/{}/gen'.format(mode)):
                obj = cc.replace('.cpp', '.o')
                f.write('build {}: cxx.{} {} || {}\n'.format(obj, mode, cc, ' '.join(serializers)))
        f.write('build seastar/build/{mode}/libseastar.a seastar/build/{mode}/apps/iotune/iotune seastar/build/{mode}/gen/http/request_parser.hh seastar/build/{mode}/gen/http/http_response_parser.hh: ninja {seastar_deps}\n'
                .format(**locals()))
        f.write('  pool = seastar_pool\n')
        f.write('  subdir = seastar\n')
        f.write('  target = build/{mode}/libseastar.a build/{mode}/apps/iotune/iotune build/{mode}/gen/http/request_parser.hh build/{mode}/gen/http/http_response_parser.hh\n'.format(**locals()))
        f.write(textwrap.dedent('''\
            build build/{mode}/iotune: copy seastar/build/{mode}/apps/iotune/iotune
            ''').format(**locals()))
    f.write('build {}: phony\n'.format(seastar_deps))
    f.write(textwrap.dedent('''\
        rule configure
          command = {python} configure.py $configure_args
          generator = 1
        build build.ninja: configure | configure.py
        rule cscope
            command = find -name '*.[chS]' -o -name "*.cc" -o -name "*.hh" | cscope -bq -i-
            description = CSCOPE
        build cscope: cscope
        rule clean
            command = rm -rf build
            description = CLEAN
        build clean: clean
        default {modes_list}
        ''').format(modes_list = ' '.join(build_modes), **globals()))
