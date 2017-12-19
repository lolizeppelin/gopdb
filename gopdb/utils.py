import psutil
from simpleutil.utils import importutils
from simpleutil.utils import systemutils

import goperation

IMPLMAP = {}

EXECUTABLEMAPS = {'mysql': systemutils.find_executable('mysqld'),
                  'redis': systemutils.find_executable('redis-server')}


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
    pids = set()
    for proc in psutil.process_iter(attrs=['pid', 'exe', 'cmdline', 'username']):
        info = proc.info
        if info.get('exe') in EXECUTABLES:
            pids.add(dict(pid=info.get('pid'),
                          exe=info.get('exe'),
                          cmdline=[cmd for cmd in info.get('cmdline')],
                          username=info.get('username')))
    return pids