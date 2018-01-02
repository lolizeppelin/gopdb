import contextlib

from sqlalchemy.pool import NullPool

from simpleutil.utils import argutils
from simpleutil.utils import attributes
from simpleutil.common.exceptions import InvalidArgument

from simpleservice.ormdb.api import model_query
from simpleservice.ormdb.argformater import connformater
from simpleservice.ormdb.engines import create_engine
from simpleservice.ormdb.tools import utils

from gopdb import common
from gopdb.api.wsgi.impl import DatabaseManagerBase
from gopdb.api.wsgi.impl import exceptions
from gopdb.api.wsgi.impl import privilegeutils
from gopdb.models import RecordDatabase
from gopdb.models import GopDatabase


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
        query = query.filter(impl='record')
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

    @contextlib.contextmanager
    def _show_database(self, session, database, **kwargs):
        """show database info"""
        _record = model_query(session, RecordDatabase,
                              filter=RecordDatabase.record_id == int(database.reflection_id)).one()
        yield _record.host, _record.port

    @contextlib.contextmanager
    def _create_database(self, session, database, **kwargs):
        zone = kwargs.get('zone', 'all')
        host = attributes.validators['type:hostname_or_ip'](kwargs.get('host'))
        port = attributes.validators['type:port'](kwargs.get('port'))
        extinfo = kwargs.get('extinfo')
        desc = kwargs.get('desc')
        _record = RecordDatabase(host=host, zone=zone, port=port, extinfo=extinfo)
        session.add(_record)
        session.flush()
        database.impl = 'record'
        database.desc = desc
        database.status = common.OK
        database.reflection_id = str(_record.record_id)
        yield host, port

    @contextlib.contextmanager
    def _delete_database(self, session, database, **kwargs):
        query = model_query(session, RecordDatabase,
                            filter=RecordDatabase.record_id == int(database.reflection_id))
        _record = query.one()
        query.delete()
        yield _record.host, _record.port

    @contextlib.contextmanager
    def _delete_slave_database(self, session, slave, masters, **kwargs):
        record_ids = [int(master.database_id) for master in masters]
        record_ids.append(slave.database_id)
        records = model_query(session, RecordDatabase, filter=RecordDatabase.record_id.in_(record_ids)).all()
        if len(records) != len(record_ids):
            raise exceptions.UnAcceptableDbError('Database record can not be found')
        host, port = None
        _masters = []
        with records:
            record = records.pop()
            if record.record_id == int(slave.database_id):
                host = record.host
                port = record.port
            else:
                for master in masters:
                    if record.record_id == int(master.database_id):
                        _masters.append((master, record.host, record.port))
                    break
        try:
            yield host, port
        except Exception:
            raise
        else:
            for m in _masters:
                privilegeutils.mysql_drop_replprivileges(m[0], slave, m[1], m[2])
        finally:
            del _masters[:]

    def _start_database(self, database, **kwargs):
        """impl start a database code"""
        raise NotImplementedError

    def _stop_database(self, database, **kwargs):
        """impl stop a database code"""
        raise NotImplementedError

    def _status_database(self, database, **kwargs):
        """impl status a database code"""
        raise NotImplementedError

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
                                    charcter_set=options.get('charcter_set'),
                                    collation_type=options.get('collation_type'))
            yield host, port
        except Exception:
            raise

    @contextlib.contextmanager
    def _copy_schema(self, session,
                     src_database, src_schema,
                     dst_database, dst_schema,
                     auths, **kwargs):
        if not dst_database.passwd:
            raise
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


    def _create_slave_database(self, *args, **kwargs):
        raise NotImplementedError
