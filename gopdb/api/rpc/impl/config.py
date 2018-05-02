from simpleutil.config import cfg

CONF = cfg.CONF


db_opts = [
    cfg.StrOpt('localroot',
               default='root',
               help='New database intance admin name login from unix socket'),
    cfg.StrOpt('localpass',
               default='gopdb',
               help='New database intance admin password login from unix socket'),
    cfg.IntOpt('affinity',
               default=1,
               min=1, max=3,
               help='local database admin password, 1 master, 2 slave, 3 both')
]


def register_opts(group):
    CONF.register_opts(db_opts, group)