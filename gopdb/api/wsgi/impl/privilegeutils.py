from sqlalchemy.pool import NullPool
from simpleservice.ormdb.engines import create_engine
from simpleservice.ormdb.argformater import noschemaconn
from simpleservice.ormdb.tools import utils
from simpleutil.common.exceptions import InvalidArgument

from gopdb import common

from gopdb.models import GopSchema

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


def mysql_replprivileges(slave, host):
    if host == 'unkonwn':
        raise InvalidArgument('Slave not on line')
    auth = dict(user='repluser-%d' % slave.database_id,
                passwd='repl-%s' % slave.passwd,
                source=host, privileges=common.REPLICATIONRIVILEGES)
    return auth
