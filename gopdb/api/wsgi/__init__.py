from simpleutil.config import cfg
from gopdb.api.wsgi.config import register_opts

from gopdb import common

CONF = cfg.CONF

register_opts(CONF.find_group(common.DB))
