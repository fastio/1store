#!/usr/bin/python3
#
# Copyright (C) 2015 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
]

apps = [
    'pedis',
    ]

tests = scylla_tests

other = [
    'iotune',
    ]

all_artifacts = apps + tests + other

arg_parser = argparse.ArgumentParser('Configure scylla')
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

cassandra_interface = Thrift(source = 'interface/cassandra.thrift', service = 'Cassandra')

scylla_core = ([
        'utils/bytes.cc',
        'utils/logalloc.cc',
        'utils/large_bitset.cc',
        'utils/runtime.cc',
        'utils/murmur_hash.cc',
        'utils/uuid.cc',
        'utils/big_decimal.cc',
        'utils/types.cc',
        'utils/utils.cc',
        'utils/UUID_gen.cc',
        'utils/rate_limiter.cc',
        'utils/file_lock.cc',
        'utils/dynamic_bitset.cc',
        'utils/managed_bytes.cc',
        'utils/exceptions.cc',
        'utils/unimplemented.cc',
        'utils/gc_clock.cc',
        'utils/disk-error-handler.cc',
        'message/messaging_service.cc',
        'exceptions/exceptions.cc',
        'idl/gossip_digest.idl.hh',
        'release.cc',
        'io/io.cc',
        'gms/application_state.cc',
        'gms/versioned_value.cc',
        'gms/endpoint_state.cc',
        'gms/gossip_digest_ack2.cc',
        'gms/gossip_digest_syn.cc',
        'gms/version_generator.cc',
        'gms/failure_detector.cc',
        'gms/gossip_digest_ack.cc',
        'gms/gossiper.cc',
        'gms/inet_address.cc',
      'redis.cc',
      'server.cc',
      'db.cc',
      'redis_protocol_parser.rl',
      'redis_protocol.cc',
      'structures/dict_lsa.cc',
      'structures/geo.cc',
      'structures/hll.cc',
      'structures/bits_operation.cc',
      'structures/list_lsa.cc',
      'cache.cc',
      'reply_builder.cc',
      'ring.cc',
      'proxy.cc',
      'service.cc',
      'config.cc',
      'init.cc',
      'token.cc',
      'keys.cc',
      'store/table/block.cc',
      'store/table/block_builder.cc',
      'store/comparator.cc',
               ]
               )

api = [
       ]

store = [
        'store/filename.cc',
        'store/util/logging.cc',
        'store/file_writer.cc',
        'store/file_reader.cc',
        ]
scylla_tests_dependencies = scylla_core + api +  [
]

scylla_tests_seastar_deps = [
]

deps = {
    'pedis': ['main.cc'] + scylla_core + store + api,
}

pure_boost_tests = set([
])

tests_not_using_seastar_test_framework = set([
]) | pure_boost_tests

for t in tests_not_using_seastar_test_framework:
    if not t in scylla_tests:
        raise Exception("Test %s not found in scylla_tests" % (t))

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

status = subprocess.call("./PEDIS-VERSION-GEN")
if status != 0:
    print('Version file generation failed')
    sys.exit(1)

file = open('build/PEDIS-VERSION-FILE', 'r')
pedis_version = file.read().strip()
file = open('build/PEDIS-RELEASE-FILE', 'r')
pedis_release = file.read().strip()

extra_cxxflags["release.cc"] = "-DPEDIS_VERSION=\"\\\"" + pedis_version + "\\\"\" -DPEDIS_RELEASE=\"\\\"" + pedis_release + "\\\"\""

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
            cxxflags_{mode} = -I. -I $builddir/{mode}/gen -I seastar -I seastar/build/{mode}/gen
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
