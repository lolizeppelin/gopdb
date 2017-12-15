from simpleutil.utils import importutils

import goperation

IMPLMAP = {}


def impl_cls(impl):
    try:
        return IMPLMAP[impl]()
    except KeyError:
        with goperation.lock('gopdb-impl'):
            cls_string = 'gopdb.impl.%s.DataBaseManager'
            cls = importutils.import_class(cls_string)
            IMPLMAP.setdefault(impl, cls)
            return cls()