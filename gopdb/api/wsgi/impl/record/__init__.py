import contextlib
from sqlalchemy.pool import NullPool

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.utils import argutils
from simpleutil.utils import attributes

from simpleservice.ormdb.api import model_query
from simpleservice.ormdb.argformater import connformater
from simpleservice.ormdb.engines import create_engine
from simpleservice.ormdb.tools import utils

from gopdb import common
from gopdb import privilegeutils
from gopdb.api import exceptions
from gopdb.api.wsgi.impl import DatabaseManagerBase
from gopdb.models import GopDatabase
from gopdb.models import RecordDatabase


class DatabaseManager(DatabaseManagerBase):

    @contextlib.contextmanager
    def _reflect_database(self, session, **kwargs):
        """impl reflect code"""
        records = kwargs.get('records')
        records = argutils.map_with(records, str)
        filter = GopDatabase.reflection_id.in_(records)
        yield 'record_id', filter

    def _select_database(self, session, query, dbtype, **kwargs):
        zone = kwargs.get('zone', 'all')
        query = query.filter_by(impl='record')
        affinitys = {}
        rquery = model_query(session, RecordDatabase, filter=RecordDatabase.zone == zone)
        includes = set()
        for r in rquery:
            r.add(r.record_id)
        if not includes:
            raise InvalidArgument('No record database found with zone %s' % zone)
        for _database in query:
            if int(_database.reflection_id) not in includes:
                continue
            try:
                affinitys[_database.affinity].append(_database)
            except KeyError:
                affinitys[_database.affinity] = [_database]
        if not affinitys:
            raise InvalidArgument('No record database found')
        result = []
        for affinity in affinitys:
            result.append(dict(affinity=affinity,
                               databases=[_database.database_id
                                          for _database in sorted(affinitys[affinity],
                                                                  key=lambda x: len(x.schemas))]
                               ))
        return result

    def _address(self, session, dbmaps):
        record_ids = map(int, dbmaps.keys())
        _records = model_query(session, RecordDatabase,
                               filter=RecordDatabase.record_id.in_(record_ids))
        address_maps = dict()
        for _record in _records:
            address_maps[dbmaps[str(_records.record_id)]] = dict(host=_record.host, port=_record.port)
        return address_maps

    @contextlib.contextmanager
    def _show_database(self, session, database, **kwargs):
        """show database info"""
        _record = model_query(session, RecordDatabase,
                              filter=RecordDatabase.record_id == int(database.reflection_id)).one()
        yield _record.host, _record.port

    @contextlib.contextmanager
    def _create_database(self, session, database, bond, **kwargs):
        zone = kwargs.get('zone', 'all')
        host = attributes.validators['type:hostname_or_ip'](kwargs.get('host'))
        port = attributes.validators['type:port'](kwargs.get('port'))
        extinfo = kwargs.get('extinfo')
        _record = RecordDatabase(host=host, zone=zone, port=port, extinfo=extinfo)
        session.add(_record)
        session.flush()
        database.impl = 'record'
        database.status = common.OK
        database.slave = kwargs.get('slave')
        database.reflection_id = str(_record.record_id)
        if bond and bond.passwd and database.passwd:
            # TODO add privileges for master and star slave
            pass
        yield host, port

    @contextlib.contextmanager
    def _delete_database(self, session, database, **kwargs):
        query = model_query(session, RecordDatabase,
                            filter=RecordDatabase.record_id == int(database.reflection_id))
        _record = query.one()
        query.delete()
        yield _record.host, _record.port

    def _start_database(self, database, **kwargs):
        """impl start a database code"""
        raise NotImplementedError

    def _stop_database(self, database, **kwargs):
        """impl stop a database code"""
        raise NotImplementedError

    def _status_database(self, database, **kwargs):
        """impl status a database code"""
        raise NotImplementedError

    def _bond_database(self, session, master, slave, relation, **kwargs):
        raise NotImplementedError('Wait!!!')
        # try:
        #     if master_host == slave_host:
        #         raise exceptions.UnAcceptableDbError('Master and Salve in same host')
        #     # master do
        #     connection = connformater % dict(user=master.user, passwd=master.passwd,
        #                                      host=master_host, port=master_port, schema='')
        #     engine = create_engine(connection, thread_checkin=False, poolclass=NullPool)
        #     with engine.connect() as conn:
        #         LOG.info('Login master database to get pos and file')
        #         r = conn.execute('show master status')
        #         results = r.fetchall()
        #         r.close()
        #     if not results:
        #         raise exceptions.UnAcceptableDbError('Master bind log not open!')
        #     binlog = results[0]
        #     if binlog.get('file')[-1] != '1' or binlog.get('position') > 1000:
        #         raise exceptions.UnAcceptableDbError('Database pos of file error')
        #     # slave do
        #     slave_info = dict(replname='database-%d' % master.database_id,
        #                       host=master_host, port=master_port,
        #                       repluser=repl.get('user'), replpasswd=repl.get('passwd'),
        #                       file=binlog.get('file'), pos=binlog.get('position'))
        #     sqls = ['SHOW SLAVE STATUS']
        #     sqls.append("CHANGE MASTER '%(replname)s' TO MASTER_HOST='%(host)s', MASTER_PORT=%(port)d," \
        #                 "MASTER_USER='%(repluser)s',MASTER_PASSWORD='%(replpasswd)s'," \
        #                 "MASTER_LOG_FILE='%(file)s',MASTER_LOG_POS=%(pos)s)" % slave_info)
        #     sqls.append('START salve %(replname)s' % slave_info)
        #
        #     connection = connformater % dict(user=slave.user, passwd=slave.passwd,
        #                                      host=slave_host, port=slave_port, schema='')
        #     engine = create_engine(connection, thread_checkin=False, poolclass=NullPool)
        #     with engine.connect() as conn:
        #         LOG.info('Login slave database for init')
        #         r = conn.execute(sqls[0])
        #         if LOG.isEnabledFor(logging.DEBUG):
        #             for row in r.fetchall():
        #                 LOG.debug(str(row))
        #         r.close()
        #         r = conn.execute(sqls[1])
        #         r.close()
        #         LOG.debug('Success add repl info')
        #         try:
        #             r = conn.execute(sqls[2])
        #         except Exception:
        #             LOG.error('Start slave fail')
        #             raise exceptions.UnAcceptableDbError('Start slave fail')
        #         else:
        #             r.close()
        # except exceptions.UnAcceptableDbError:
        #     raise
        # except Exception as e:
        #     if LOG.isEnabledFor(logging.DEBUG):
        #         LOG.exception('Bond slave fail')
        #     raise exceptions.UnAcceptableDbError('Bond slave fail with %s' % e.__class__.__name__)

    def _unbond_database(self, session, master, slave, relation, **kwargs):
        """impl unbond slave database"""
        raise NotImplementedError('Wait!!!')

    def _revoke_database_user(self, database, auth, **kwargs):
        """impl unbond slave database"""
        raise NotImplementedError('Wait!!!')

    def _slave_database(self, session, master, slave, **kwargs):
        raise NotImplementedError('Wait!!!')

    def _ready_relation(self, session, master, slave, relation, **kwargs):
        raise NotImplementedError('Wait!!!')

    @contextlib.contextmanager
    def _show_schema(self, session, database, schema, **kwargs):
        _record = model_query(session, RecordDatabase,
                              filter=RecordDatabase.record_id == int(database.reflection_id)).one()
        host = _record.host
        port = _record.port
        yield host, port

    @contextlib.contextmanager
    def _create_schema(self, session,
                       database, schema, auths, options, **kwargs):
        """create new schema intance on database_id"""
        try:
            _record = model_query(session, RecordDatabase,
                                  filter=RecordDatabase.record_id == int(database.reflection_id)).one()
            host = _record.host
            port = _record.port
            if database.passwd:
                connection = connformater % dict(user=database.user, passwd=database.passwd,
                                                 host=host, port=port, schema=schema)
                engine = create_engine(connection, thread_checkin=False, poolclass=NullPool)
                utils.create_schema(engine, auths=auths,
                                    character_set=options.get('character_set'),
                                    collation_type=options.get('collation_type'),
                                    connection_timeout=3)
            yield host, port
        except Exception:
            raise

    @contextlib.contextmanager
    def _copy_schema(self, session,
                     src_database, src_schema,
                     dst_database, dst_schema,
                     auths, **kwargs):
        src_record = model_query(session, RecordDatabase,
                                 filter=RecordDatabase.record_id == int(src_database.reflection_id)).one()
        dst_record = model_query(session, RecordDatabase,
                                 filter=RecordDatabase.record_id == int(dst_database.reflection_id)).one()

        src_info = dict(user=src_database.user, passwd=src_database.passwd,
                        host=src_record.host, port=src_record.port)
        dst_info = dict(user=dst_database.user, passwd=dst_database.passwd,
                        host=dst_record.host, port=dst_record.port)
        schema_info = utils.copydb(src=src_info,
                                   dst=dst_info,
                                   auths=auths, tables_need_copy=kwargs.get('tables_need_copy'),
                                   exec_sqls=kwargs.get('exec_sqls'))
        try:
            yield schema_info[1], schema_info[2]
        except Exception:
            engine = create_engine(connformater % dst_info,
                                   thread_checkin=False,
                                   poolclass=NullPool)
            utils.drop_schema(engine, auths)
            raise

    @contextlib.contextmanager
    def _delete_schema(self, session, database, schema, **kwargs):
        """delete schema intance on reflection_id"""
        _record = model_query(session, RecordDatabase,
                              filter=RecordDatabase.record_id == int(database.reflection_id)).one()
        host = _record.host
        port = _record.port
        if database.passwd:
            connection = connformater % dict(user=database.user, passwd=database.passwd,
                                             schema=schema.schema,
                                             host=host, port=port)
            engine = create_engine(connection, thread_checkin=False, poolclass=NullPool)
            dropauths = None
            if schema.user != database.user:
                dropauths = privilegeutils.mysql_privileges(schema)
            utils.drop_schema(engine, dropauths)
        yield host, port
