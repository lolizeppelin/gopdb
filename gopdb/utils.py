from simpleutil.utils import importutils

import goperation

IMPLMAP = {}


def impl_cls(api, impl):
    try:
        return IMPLMAP[impl]()
    except KeyError:
        with goperation.lock.get('gopdb-impl-map'):
            cls_string = 'gopdb.%s.impl.%s.DatabaseManager' % (api, impl)
            cls = importutils.import_class(cls_string)
            IMPLMAP.setdefault(impl, cls)
            return cls()