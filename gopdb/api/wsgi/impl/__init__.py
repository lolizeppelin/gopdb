import abc

import six

from gopdb import common
from gopdb.api import endpoint_session
from gopdb.api.wsgi.impl import exceptions
from gopdb.api.wsgi.impl import privilegeutils
from gopdb.models import GopDatabase
from gopdb.models import GopSalveRelation
from gopdb.models import GopSchema
from gopdb.models import SchemaQuote
from simpleservice.ormdb.api import model_count_with_key
from simpleservice.ormdb.api import model_query
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import and_


@six.add_metaclass(abc.ABCMeta)
class DatabaseManagerBase(object):

    # ----------database action-------------

    def reflect_database(self, **kwargs):
        session = endpoint_session(readonly=True)
        _result = []
        with self._reflect_database(session, **kwargs) as filters:
            key = filters[0]
            _filter = filters[1]
            if _filter is None:
                return _result
            query = model_query(session, GopDatabase, filter=_filter)
            for _database in query:
                dbinfo = dict(database_id=_database.database_id)
                dbinfo.setdefault(key, _database.reflection_id)
                _result.append(dbinfo)
        return _result

    @abc.abstractmethod
    def _reflect_database(self, session, **kwargs):
        """impl reflect code"""

    def show_database(self, database_id, **kwargs):
        """show database info"""
        session = endpoint_session(readonly=True)
        query = model_query(session, GopDatabase, filter=GopDatabase.database_id == database_id)
        _database = query.one()
        if _database.is_master:
            schemas = _database.schemas
        else:
            # slave will find  masters schemas to show
            master_ids = model_query(session, GopSalveRelation.master_id,
                                     filter=GopDatabase.slave_id == database_id).all()
            query = model_query(session, GopDatabase, filter=and_(GopDatabase.database_id.in_(master_ids),
                                                                  GopDatabase.is_master == False))
            query = query.options(joinedload(GopDatabase.schemas, innerjoin=False))
            schemas = []
            for m_database in query.all():
                schemas.extend(m_database.schemas)
        _result = dict(database_id=database_id,
                       impl=_database.impl,
                       dbtype=_database.dbtype,
                       dbversion=_database.dbversion,
                       schemas=[dict(schema=schema.schema,
                                     schema_id=schema.schema_id
                                     ) for schema in schemas],
                       quotes=[dict(entity=quote.entity,
                                    endpoint=quote.endpoint,
                                    quote_id=quote.quote_id,
                                    schema_id=quote.schema_id)
                               for quote in _database.quotes])
        if _database.is_master:
            # show master database slaves
            _result.setdefault('slaves', _database.slaves)
        with self._show_database(session, _database, **kwargs) as address:
            host = address[0]
            port = address[1]
            _result.setdefault('host', host)
            _result.setdefault('port', port)
        return _result

    @abc.abstractmethod
    def _show_database(self, session, database, **kwargs):
        """impl show code"""

    def create_database(self, user, passwd, dbtype, dbversion, affinity, **kwargs):
        """create new database intance"""
        session = endpoint_session()
        with session.begin():
            _database = GopDatabase(user=user, passwd=passwd, is_master=True,
                                    dbtype=dbtype, dbversion=dbversion, affinity=affinity,
                                    desc=kwargs.pop('desc', None))
            _result = dict(dbversion=_database.dbversion,
                           dbtype=_database.dbtype)
            with self._create_database(session, _database, **kwargs) as address:
                host = address[0]
                port = address[1]
                _result.setdefault('host', host)
                _result.setdefault('port', port)
                if not _database.is_master:
                    raise exceptions.UnAcceptableDbError('Can not add slave database from api create database')
                session.add(_database)
                session.flush()
        self._esure_create(_database, **kwargs)
        _result.setdefault('database_id', _database.database_id)
        return _result

    @abc.abstractmethod
    def _create_database(self, session, database, **kwargs):
        """impl create code"""

    def _esure_create(self, database, **kwargs):
        """impl esure create result"""

    def delete_database(self, database_id, **kwargs):
        """delete master database intance"""
        session = endpoint_session()
        query = model_query(session, GopDatabase, filter=GopDatabase.database_id == database_id)
        query = query.options(joinedload(GopDatabase.schemas, innerjoin=False))
        with session.begin():
            _database = query.one()
            _result = dict(database_id=_database.database_id, impl=_database.impl, dbtype=_database.dbtype,
                           dbversion=_database.dbversion)
            if not _database.is_master:
                raise exceptions.AcceptableDbError('can not delete slave database from this api')
            if _database.schemas or _database.slaves:
                raise exceptions.AcceptableDbError('can not delete database, slave or schema exist')
            if model_count_with_key(session, SchemaQuote.database_id,
                                    filter=SchemaQuote.database_id == _database.database_id):
                raise exceptions.AcceptableDbError('Database in schema quote list')
            with self._delete_database(session, _database, **kwargs) as address:
                host = address[0]
                port = address[1]
                _result.setdefault('host', host)
                _result.setdefault('port', port)
                query.delete()
        return _result

    @abc.abstractmethod
    def _delete_database(self, session, database):
        """impl delete database code"""

    def create_slave_database(self, database_id, **kwargs):
        """create a slave database for database with database_id"""
        raise NotImplementedError

    @abc.abstractmethod
    def _create_slave_database(self, *args, **kwargs):
        raise NotImplementedError

    def delete_slave_database(self, database_id, **kwargs):
        """delete a slave database"""
        session = endpoint_session()
        query = model_query(session, GopDatabase, filter=GopDatabase.database_id == database_id)
        query = query.options(joinedload(GopDatabase.quotes, innerjoin=False))
        with session.begin():
            slave = query.one_or_none()
            _result = dict(database_id=slave.database_id, impl=slave.impl, dbtype=slave.dbtype,
                           dbversion=slave.dbversion)
            if slave.is_master:
                raise exceptions.AcceptableDbError('Target database is not a slave database')
            if slave.quotes:
                raise exceptions.AcceptableDbError('Target database in schema quote list')
            _masters = [m[0] for m in model_query(session, GopSalveRelation.master_id,
                                                  filter=GopSalveRelation.slave_id == database_id).all()]
            if not _masters:
                raise exceptions.UnAcceptableDbError('Target slave database can not find master')
            masters = model_query(session, GopDatabase, filter=and_(GopDatabase.database_id.in_(_masters),
                                                                    GopDatabase.is_master == True))
            if len(_masters) != len(masters):
                raise exceptions.UnAcceptableDbError('Target slave database master missed')
            with self._delete_slave_database(session, slave, masters) as address:
                query.delete()
                host = address[0]
                port = address[1]
                _result.setdefault('host', host)
                _result.setdefault('port', port)
        return _result

    @abc.abstractmethod
    def _delete_slave_database(self, session, slave, masters, **kwargs):
        """impl delete a slave database code"""

    # ----------schema action-------------

    def show_schema(self, database_id, schema, secret, **kwargs):
        """show schema info"""
        session = endpoint_session()
        query = model_query(session, GopDatabase, filter=GopDatabase.database_id == database_id)
        query = query.options(joinedload(GopDatabase.schemas, innerjoin=False))
        _database = query.one()
        if not _database.is_master:
            raise exceptions.AcceptableDbError('Database is slave, can not get schema')
        _schema = None
        for __schema in _database.schemas:
            if __schema.schema == schema:
                _schema = __schema
                break
        if not _schema:
            raise exceptions.AcceptableSchemaError('Schema not not be found in %d' % database_id)
        _result = dict(database_id=database_id,
                       impl=_database.impl,
                       dbtype=_database.dbtype,
                       dbversion=_database.dbversion,
                       schema=_schema.schema,
                       schema_id=_schema.schema_id,
                       desc=_schema.desc)
        if secret:
            _result.update({'user': _schema.user,
                            'passwd': _schema.passwd,
                            'ro_user': _schema.ro_user,
                            'ro_passwd': _schema.ro_passwd})
        with self._show_schema(session, _database, _schema, **kwargs) as address:
            host = address[0]
            port = address[1]
            _result.setdefault('host', host)
            _result.setdefault('port', port)
        return _result

    @abc.abstractmethod
    def _show_schema(self, session, database, schema, **kwargs):
        """impl show schema code"""

    def create_schema(self, database_id, schema, auth, options, **kwargs):
        """create new schema intance on reflection_id"""
        auths = privilegeutils.mysql_privileges(auth)
        bond = kwargs.get('bond')
        session = endpoint_session()
        query = model_query(session, GopDatabase, filter=GopDatabase.database_id == database_id)
        query = query.options(joinedload(GopDatabase.schemas, innerjoin=False))
        quote_id = 0
        with session.begin():
            _database = query.one()
            _result = dict(database_id=database_id, impl=_database.impl,
                           dbtype=_database.dbtype, dbversion=_database.dbversion)
            if not _database.is_master:
                raise exceptions.AcceptableDbError('Database is slave, can not create schema')
            if _database.status != common.OK:
                raise exceptions.AcceptableDbError('Database is not OK now')
            schemas = [_schema.schema for _schema in _database.schemas]
            if schema in schemas:
                raise exceptions.AcceptableDbError('Duplicate schema name Duplicate')
            options = options or {'charcter_set': 'utf8'}
            with self._create_schema(session, _database, schema, auths, options, **kwargs) as address:
                gop_schema = GopSchema(schema=schema,
                                       database_id=_database.database_id,
                                       user=auth.get('user'),
                                       passwd=auth.get('passwd'),
                                       ro_user=auth.get('ro_user'),
                                       ro_passwd=auth.get('ro_passwd'),
                                       source=auth.get('source'),
                                       charcter_set=options.get('charcter_set'),
                                       collation_type=options.get('collation_type'))
                session.add(gop_schema)
                session.flush()
                if bond:
                    _quote = SchemaQuote(schema_id=gop_schema.schema_id,
                                         database_id=_database.database_id,
                                         entity=bond.get('entity'),
                                         endpoint=bond.get('endpoint'),
                                         desc=bond.get('desc'))
                    session.add(_quote)
                    session.flush()
                    quote_id = _quote.quote_id
                host = address[0]
                port = address[1]
                _result.setdefault('host', host)
                _result.setdefault('port', port)
                _result.setdefault('charcter_set', options.get('charcter_set'))
                _result.setdefault('collation_type', options.get('collation_type'))
                _result.setdefault('schema_id', gop_schema.schema_id)
                _result.setdefault('schema', gop_schema.schema)
                _result.setdefault('quote_id', quote_id)
        return _result

    @abc.abstractmethod
    def _create_schema(self, session, database, schema, auths, options, **kwargs):
        """impl create new schema code"""

    def copy_schema(self, src_database_id, src_schema,
                    dst_database_id, dst_schema, auth,
                    **kwargs):
        """create a schema"""
        auths = privilegeutils.mysql_privileges(auth)
        session = endpoint_session()
        query = model_query(session, GopDatabase,
                            filter=GopDatabase.database_id.in_([src_database_id, dst_database_id]))
        query = query.options(joinedload(GopDatabase.schemas, innerjoin=False))
        src_database, dst_database = None, None
        for _database in query.all():
            if _database.database_id == src_database_id:
                if not _database.is_master:
                    raise
                if not _database.passwd:
                    raise
                schemas = [_schema.name for _schema in _database.schemas]
                if src_schema not in schemas:
                    raise
                src_database = _database
            elif _database.database_id == dst_database_id:
                if not _database.is_master:
                    raise
                if not _database.passwd:
                    raise
                schemas = [_schema.name for _schema in _database.schemas]
                if dst_schema in schemas:
                    raise
                dst_database = _database
        if not src_database or not dst_database:
            raise
        _result = dict(database_id=dst_database.database_id,
                       impl=dst_database.impl, dbtype=dst_database.dbtype,
                       dbversion=dst_database.dbversion)
        with session.begin():
            with self._copy_schema(session,
                                   src_database, src_schema,
                                   dst_database, dst_schema,
                                   auths, **kwargs) as options:
                charcter_set = options[0]
                collation_type = options[1]
                gop_schema = GopSchema(schema=dst_schema,
                                       database_id=dst_database.database_id,
                                       user=auth.get('user'),
                                       passwd=auth.get('passwd'),
                                       ro_user=auth.get('ro_user'),
                                       ro_passwd=auth.get('ro_passwd'),
                                       source=auth.get('source'),
                                       character=charcter_set,
                                       collation=collation_type)
                session.add(gop_schema)
                session.flush()
                _result.setdefault('schema_id', gop_schema.schema_id)
                _result.setdefault('schema', gop_schema.schema)
        return _result

    @abc.abstractmethod
    def _copy_schema(self, session,
                     src_database, src_schema,
                     dst_database, dst_schema,
                     auths, **kwargs):
        """impl copy schema code"""

    def delete_schema(self, database_id, schema, **kwargs):
        """delete schema intance on reflection_id"""
        unquotes = set(kwargs.get('unquotes', []))
        session = endpoint_session()
        query = model_query(session, GopDatabase, filter=GopDatabase.database_id == database_id)
        query = query.options(joinedload(GopDatabase.schemas, innerjoin=False))
        with session.begin():
            _database = query.one()
            _result = dict(database_id=_database.database_id,
                           impl=_database.impl, dbtype=_database.dbtype,
                           dbversion=_database.dbversion)
            if not _database.is_master:
                raise exceptions.AcceptableDbError('can not delete schema from slave database')
            squery = model_query(session, GopSchema, filter=and_(GopSchema.schema == schema,
                                                                 GopSchema.database_id == database_id))
            squery = squery.options(joinedload(GopSchema.quotes, innerjoin=False))
            _schema = squery.one()
            if _schema.quotes:
                if not unquotes == set([_quote.quote_id for _quote in _schema.quotes]):
                    raise exceptions.AcceptableSchemaError('Schema in quote, can not be delete')
            with self._delete_schema(session, _database, _schema, **kwargs) as address:
                host = address[0]
                port = address[1]
                _result.setdefault('host', host)
                _result.setdefault('port', port)
                _result.setdefault('schema', schema)
                squery.delete()
        return _result

    @abc.abstractmethod
    def _delete_schema(self, session, database, schema, **kwargs):
        """impl delete schema intance code"""
