import rpm
import psutil
from simpleutil.utils import importutils
from simpleutil.utils import systemutils

import goperation

from gopdb import common

IMPLMAP = {}
EXECUTABLEMAPS = {}


def mysql_version():
    ts = rpm.ts()
    mi = ts.dbMatch(rpm.RPMTAG_PROVIDES, 'mysql-server')
    try:
        if not mi:
            return None
        for hdr in mi:
            # for _file in h[rpm.RPMTAG_FILENAMES]:
            #    print _file
            name = hdr[rpm.RPMTAG_NAME].lower()
            version = hdr[rpm.RPMTAG_VERSION]
            if 'mariadb' in name:
                if version >= '10.0.0':
                    return '5.6.0'
                return version
            return version
    finally:
        del mi
        ts.closeDB()

def redis_version():
    ts = rpm.ts()
    mi = ts.dbMatch(rpm.RPMTAG_PROVIDES, 'redis')
    try:
        if not mi:
            return None
        for hdr in mi:
            version = hdr[rpm.RPMTAG_VERSION]
            return version
    finally:
        del mi
        ts.closeDB()


common.VERSIONMAP.setdefault('mysql', mysql_version())
common.VERSIONMAP.setdefault('redis', redis_version())


try:
    EXECUTABLEMAPS.setdefault('mysql', systemutils.find_executable('mysqld'))
except NotImplementedError:
    pass

try:
    EXECUTABLEMAPS.setdefault('redis', systemutils.find_executable('redis-server'))
except NotImplementedError:
    pass


def impl_cls(api, impl):
    try:
        return IMPLMAP[impl]()
    except KeyError:
        with goperation.lock.get('gopdb-impl-map'):
            cls_string = 'gopdb.api.%s.impl.%s.DatabaseManager' % (api, impl)
            cls = importutils.import_class(cls_string)
            IMPLMAP.setdefault(impl, cls)
            return cls()


def find_process(impl=None):
    if impl:
        EXECUTABLES = [EXECUTABLEMAPS[impl], ]
    else:
        EXECUTABLES = EXECUTABLEMAPS.values()
    pids = []
    for proc in psutil.process_iter(attrs=['pid', 'exe', 'cmdline', 'username']):
        info = proc.info
        if info.get('exe') in EXECUTABLES:
            pids.append(dict(pid=info.get('pid'),
                             exe=info.get('exe'),
                             cmdline=[cmd for cmd in info.get('cmdline')],
                             username=info.get('username')))
    return pids