import random
import string
from sqlalchemy.pool import NullPool
from simpleservice.ormdb.engines import create_engine
from simpleservice.ormdb.argformater import noschemaconn
from simpleservice.ormdb.tools import utils
from simpleutil.common.exceptions import InvalidArgument

from gopdb import common

from gopdb.models import GopSchema
from gopdb.models import GopDatabase

src = string.ascii_lowercase

def mysql_privileges(auth):
    if isinstance(auth, dict):
        return [{'user': auth.get('user'), 'passwd': auth.get('passwd'),
                 'source': auth.get('source') or '%',
                 'privileges': common.ALLPRIVILEGES},
                {'user': auth.get('ro_user'), 'passwd': auth.get('ro_passwd'),
                 'source': auth.get('rosource') or '%',
                 'privileges': common.READONLYPRIVILEGES}]
    elif isinstance(auth, GopSchema):
        return [{'user': auth.user,
                 'passwd': auth.passwd,
                 'source': auth.source or '%',
                 'privileges': common.ALLPRIVILEGES},
                {'user': auth.ro_user,
                 'passwd': auth.ro_passwd,
                 'source': auth.rosource or '%',
                 'privileges': common.READONLYPRIVILEGES}]
    else:
        raise TypeError


def mysql_drop_replprivileges(master, slave, host, port):
    if host == 'unkonwn':
        raise InvalidArgument('Slave not on line')
    _connection = noschemaconn % dict(user=master.user, passwd=master.passwd,
                                      host=host, port=port)
    engine = create_engine(_connection, thread_checkin=False,
                           poolclass=NullPool)
    auth = dict(user='repluser-%d' % slave.database_id,
                passwd='repl-%s' % slave.passwd,
                source=host, privileges=common.REPLICATIONRIVILEGES)
    utils.drop_privileges(engine, auths=[auth, ])


def mysql_replprivileges(database_id, host, **kwargs):
    auth = dict(user='repluser-%d' % database_id,
                source=host, privileges=common.REPLICATIONRIVILEGES)
    auth.update(kwargs)
    if not auth.get('passwd'):
        auth['passwd'] = ''.join(random.sample(string.ascii_lowercase, 6))
    auth['database_id'] = database_id
    auth['host'] = host
    return auth


def mysql_slave_replprivileges(slave_id, **kwargs):
    auth = mysql_replprivileges(**kwargs)
    auth['user'] = 'repluser-%d' % slave_id
    return auth
