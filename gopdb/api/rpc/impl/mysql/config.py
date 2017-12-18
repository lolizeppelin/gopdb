from simpleutil.config import cfg

CONF = cfg.CONF


mysql_db_opts = [
    cfg.IntOpt('expire_log_days',
               min=1,
               max=14,
               default=3,
               help='mysql bin log expire days'),
]


def register_opts(group):
    CONF.register_opts(mysql_db_opts, group)