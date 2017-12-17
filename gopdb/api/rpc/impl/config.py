from simpleutil.config import cfg

CONF = cfg.CONF


db_opts = [
    cfg.StrOpt('localroot',
               default='root',
               help='local database login admin name'),
    cfg.StrOpt('localpass',
               default='gopdb',
               help='local database admin password')
]


def register_opts(group):
    CONF.register_opts(db_opts, group)