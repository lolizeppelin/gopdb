# -*- coding:utf-8 -*-
import re
import webob.exc

from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import argutils
from simpleutil.utils import jsonutils
from simpleutil.utils import singleton

from simpleservice.ormdb.api import model_query
from simpleservice.rpc.exceptions import AMQPDestinationNotFound
from simpleservice.rpc.exceptions import MessagingTimeout
from simpleservice.rpc.exceptions import NoSuchMethod

from goperation.manager.exceptions import CacheStoneError
from goperation.manager.utils import resultutils
from goperation.manager.wsgi.contorller import BaseContorller
from goperation.manager.wsgi.cache.controller import CacheReuest
from goperation.manager.wsgi.endpoint.controller import EndpointReuest
from goperation.manager.wsgi.entity.controller import EntityReuest
from goperation.manager.wsgi.exceptions import RpcPrepareError
from goperation.manager.wsgi.exceptions import RpcResultError

from gopdb import common
from gopdb import utils
from gopdb.api import endpoint_session
from gopdb.api import exceptions
from gopdb.api.wsgi.impl import _address
from gopdb.api.wsgi.impl import _impl
from gopdb.models import GopDatabase
from gopdb.models import GopSchema
from gopdb.models import SchemaQuote


safe_dumps = jsonutils.safe_dumps_as_bytes
safe_loads = jsonutils.safe_loads_as_bytes

LOG = logging.getLogger(__name__)

FAULT_MAP = {InvalidArgument: webob.exc.HTTPClientError,
             NoSuchMethod: webob.exc.HTTPNotImplemented,
             AMQPDestinationNotFound: webob.exc.HTTPServiceUnavailable,
             MessagingTimeout: webob.exc.HTTPServiceUnavailable,
             RpcResultError: webob.exc.HTTPInternalServerError,
             CacheStoneError: webob.exc.HTTPInternalServerError,
             RpcPrepareError: webob.exc.HTTPInternalServerError,
             NoResultFound: webob.exc.HTTPNotFound,
             MultipleResultsFound: webob.exc.HTTPInternalServerError,
             exceptions.AcceptableDbError: webob.exc.HTTPClientError,
             exceptions.AcceptableSchemaError: webob.exc.HTTPClientError,
             exceptions.UnAcceptableDbError: webob.exc.HTTPInternalServerError,
             exceptions.UnAcceptableSchemaError: webob.exc.HTTPInternalServerError,
             }

entity_controller = EntityReuest()
endpoint_controller = EndpointReuest()
cache_controller = CacheReuest()


@singleton.singleton
class DatabaseReuest(BaseContorller):

    CREATEDATABASE = {'type': 'object',
                      'required': ['impl', 'dbtype', 'user', 'passwd', 'slave'],
                      'properties': {
                          'impl': {'type': 'string', 'description': '实现方式'},
                          'dbtype': {'type': 'string', 'description': '数据库类型,目前支持mysql'},
                          'user': {'type': 'string', 'description': '数据库root用户名'},
                          'passwd': {'type': 'string', 'description': '数据库root密码'},
                          'slave': {'type': 'integer', 'description': '0表示主库, >0 表示从库可以接受的主库数量'},
                          'bond': {'type': 'integer', 'description': '表示要绑定的从库ID'},
                          'dbversion': {'type': 'object', 'description': '数据库版本'},
                          'affinity': {'type': 'integer', 'description': '数据库亲和性数值'},
                          'agent_id': {'type': 'integer', 'minimum': 0,
                                       'description': '数据库实例安装的目标机器,不填自动分配'},
                          'zone': {'type': 'string', 'description': '自动分配的安装区域,默认zone为all'},}
                      }

    BONDMASTER = {
        'type': 'object',
        'required': ['master', 'host', 'port', 'passwd'],
        'properties': {
            'master': {'type': 'integer', 'minimum': 1, 'description': '主库ID'},
            'host': {'type': 'string', 'minLength': 1, 'description': '主库host'},
            'port': {'type': 'integer', 'minimum': 1, 'description': '主库port'},
            'passwd': {'type': 'string', 'minLength': 1, 'description': '同步用户密码'},
            'file': {'type': 'string', 'minLength': 5, 'description': '主库binlog 文件名'},
            'position': {'type': 'integer', 'minimum': 1, 'description': '主库binlog 位置'},
            'force': {'type': 'boolean', 'description': '强制绑定, 忽略slave检查'},
            'schemas': {'type': 'array', 'description': '主数据库scheam列表',
                        'items': {'type': 'string', 'minLength': 1, 'description': '主数据库scheam名'}},
        }
    }

    UNBONDMASTER = {
        'type': 'object',
        'required': ['master'],
        'properties': {
            'master': {'type': 'integer', 'minimum': 1, 'description': '主库ID'},
            'force': {'type': 'boolean', 'description': '强制解绑, 忽略绑定检查'},
            'schemas': {'type': 'array', 'minItems': 1, 'description': '数据库scheam列表',
                        'items': {'type': 'string', 'minLength': 1, 'description': '数据库scheam名'}},
        }
    }

    BONDSLAVE = {
        'type': 'object',
        'required': ['slave'],
        'properties': {
            'slave': {'type': 'integer', 'minimum': 1, 'description': '从库ID'},
            'file': {'type': 'string', 'minLength': 5, 'description': '主库binlog 文件名,主库无内容时可不填写'},
            'position': {'type': 'integer', 'minimum': 1, 'description': '主库binlog 位置,主库无内容时可不填写'},
        }
    }

    SLAVEREADY = {
        'type': 'object',
        'required': ['slave'],
        'properties': {
            'slave': {'type': 'integer', 'minimum': 1, 'description': '从库ID'},
            'force': {'type': 'boolean', 'description': '忽略主从同步检查直接设置为ready'},
        }
    }

    def reflect(self, req, impl, body=None):
        body = body or {}
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = utils.impl_cls('wsgi', impl)
        reflect_list = dbmanager.reflect_database(**kwargs)
        return resultutils.results(result='reflect database success', data=reflect_list)

    def select(self, req, impl, body=None):
        body = body or {}
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = utils.impl_cls('wsgi', impl)
        dbresult = dbmanager.select_database(**kwargs)
        return resultutils.results(result='select database success', data=dbresult)

    def agents(self, req, body=None):
        body = body or {}
        kwargs = dict(req=req)
        kwargs.update(body)
        dbtype = body.pop('dbtype', 'mysql') or 'mysql'
        dbmanager = utils.impl_cls('wsgi', 'local')
        dbresult = dbmanager.select_agents(dbtype, **kwargs)
        return resultutils.results(result='select database agents success', data=dbresult)

    def index(self, req, body=None):
        body = body or {}
        order = body.pop('order', None)
        desc = body.pop('desc', False)
        page_num = int(body.pop('page_num', 0))

        slaves = body.pop('slaves', False)
        # schemas = body.pop('schemas', False)
        # quotes = body.pop('quotes', False)
        impl = body.get('impl', None)
        session = endpoint_session(readonly=True)
        _filter = None
        if impl:
            _filter = GopDatabase.impl == impl

        columns=[GopDatabase.database_id,
                 GopDatabase.slave,
                 GopDatabase.impl,
                 GopDatabase.dbtype,
                 GopDatabase.dbversion,
                 GopDatabase.reflection_id,
                 GopDatabase.status,
                 GopDatabase.affinity,
                 GopDatabase.desc]

        option = None
        if slaves:
            columns.append(GopDatabase.slaves)
            option = joinedload(GopDatabase.slaves, innerjoin=False)

        results = resultutils.bulk_results(session,
                                           model=GopDatabase,
                                           columns=columns,
                                           counter=GopDatabase.database_id,
                                           order=order, desc=desc,
                                           option=option,
                                           filter=_filter,
                                           page_num=page_num)
        for column in results['data']:

            slaves = column.get('slaves', [])
            column['slaves'] = []
            for slave in slaves:
                column['slaves'].append(dict(slave_id=slave.slave_id,
                                             master_id=column.get('database_id'),
                                             readonly=slave.readonly, ready=slave.ready))
        return results

    def create(self, req, body=None):
        body = body or {}
        jsonutils.schema_validate(body, self.CREATEDATABASE)
        impl = body.pop('impl')
        dbtype = body.pop('dbtype')
        user = body.pop('user')
        passwd = body.pop('passwd')
        dbversion = body.pop('dbversion', None)
        affinity = body.pop('affinity', 0)
        if body.get('slave'):
            if body.get('bond'):
                raise InvalidArgument('Slave database can not bond to another database ')
            affinity = 0
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = utils.impl_cls('wsgi', impl)
        dbresult = dbmanager.create_database(user, passwd, dbtype, dbversion, affinity, **kwargs)
        return resultutils.results(result='create database success', data=[dbresult, ])

    def show(self, req, database_id, body=None):
        body = body or {}
        database_id = int(database_id)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.show_database(database_id, **kwargs)
        return resultutils.results(result='show database success', data=[dbresult, ])

    def update(self, req, database_id, body=None):
        body = body or {}
        status = body.get('status', common.UNACTIVE)
        if status not in (common.UNACTIVE, common.OK):
            raise InvalidArgument('Status value error')
        database_id = int(database_id)
        session = endpoint_session()
        query = model_query(session, GopDatabase, filter=GopDatabase.database_id == database_id)
        with session.begin():
            updata = {'status': status}
            if body.get('dbversion'):
                updata.setdefault('dbversion', body.get('dbversion'))
            count = query.update(updata)
            if not count:
                LOG.warning('Update not match, no database has been updated')
        return resultutils.results(result='Update %s database success' % database_id)

    def delete(self, req, database_id, body=None):
        body = body or {}
        master = body.pop('master', False)
        database_id = int(database_id)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.delete_database(database_id, master, **kwargs)
        return resultutils.results(result='delete database success', data=[dbresult, ])

    def start(self, req, database_id, body=None):
        body = body or {}
        database_id = int(database_id)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.start_database(database_id, **kwargs)
        return resultutils.results(result='start database success', data=[dbresult, ])

    def stop(self, req, database_id, body=None):
        body = body or {}
        database_id = int(database_id)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.stop_database(database_id, **kwargs)
        return resultutils.results(result='stop database success', data=[dbresult, ])

    def status(self, req, database_id, body=None):
        body = body or {}
        database_id = int(database_id)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.status_database(database_id, **kwargs)
        return resultutils.results(result='status database success', data=[dbresult, ])

    def bond(self, req, database_id, body=None):
        """slave bond master"""
        body = body or {}
        jsonutils.schema_validate(body, self.BONDMASTER)
        database_id = int(database_id)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.bond_database(database_id, **kwargs)
        return resultutils.results(result='slave database success', data=[dbresult, ])

    def unbond(self, req, database_id, body=None):
        """slave unbond master"""
        body = body or {}
        jsonutils.schema_validate(body, self.UNBONDMASTER)
        database_id = int(database_id)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.unbond_database(database_id, **kwargs)
        return resultutils.results(result='unbond slave database success', data=[dbresult, ])

    def slave(self, req, database_id, body=None):
        """master slave(bond) a slave database"""
        body = body or {}
        jsonutils.schema_validate(body, self.BONDSLAVE)
        database_id = int(database_id)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.slave_database(database_id, **kwargs)
        return resultutils.results(result='master bond slave database success', data=[dbresult, ])

    def ready(self, req, database_id, body=None):
        body = body or {}
        jsonutils.schema_validate(body, self.SLAVEREADY)
        database_id = int(database_id)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.ready_relation(database_id, **kwargs)
        return resultutils.results(result='Set relation to ready success', data=[dbresult, ])

    @staticmethod
    def slaves_address(databases):
        dbmanager = None
        databases = set(databases)
        for database_id in databases:
            _dbmanager = _impl(database_id)
            if dbmanager is None:
                dbmanager = _dbmanager
                continue
            if dbmanager is not _dbmanager:
                raise InvalidArgument('Database impl not the same')
        return dbmanager.slaves_address(databases)


@singleton.singleton
class SchemaReuest(BaseContorller):

    AUTHSCHEMA = {
        'type': 'object',
        'required': ['user', 'passwd', 'ro_user', 'ro_passwd'],
        'properties':
            {
                'user': {'type': 'string'},
                'passwd': {'type': 'string'},
                'ro_user': {'type': 'string'},
                'ro_passwd': {'type': 'string'},
                'source': {'type': 'string'},
                'rosource': {'type': 'string'},
            }
    }

    OPTSCHEMA = {
        'type': 'object',
        'properties':
            {
                'character_set': {'type': 'string'},
                'collation_type': {'type': 'string'},
            }
    }

    CREATESCHEMA = {'type': 'object',
                    'required': ['schema', 'auth'],
                    'properties': {
                        'auth': AUTHSCHEMA,
                        'options': OPTSCHEMA,
                        'schema': {'type': 'string'},
                        'bind': {'type': 'object',
                                 'required': ['entity', 'endpoint'],
                                 'properties': {'entity': {'type': 'integer', 'minimum': 1},
                                                'endpoint': {'type': 'string'},
                                                'desc': {'type': 'string'}}
                                 }
                        }
                    }

    SCHEMAREG = re.compile('^[a-z][a-z0-9_]+$', re.IGNORECASE)

    def _validate_schema(self, schema):
        if not schema or not re.match(self.SCHEMAREG, schema):
            raise InvalidArgument('Schema name %s not match' % schema)

    def index(self, req, database_id, body=None):
        body = body or {}
        database_id = int(database_id)
        order = body.pop('order', None)
        desc = body.pop('desc', False)
        page_num = int(body.pop('page_num', 0))

        session = endpoint_session(readonly=True)
        results = resultutils.bulk_results(session,
                                           model=GopSchema,
                                           columns=[GopSchema.schema_id,
                                                    GopSchema.schema,
                                                    GopSchema.database_id,
                                                    GopSchema.schema,
                                                    GopSchema.character_set,
                                                    GopSchema.collation_type,
                                                    ],
                                           counter=GopSchema.schema_id,
                                           order=order, desc=desc,
                                           filter=GopSchema.database_id == database_id,
                                           page_num=page_num)
        return results

    def create(self, req, database_id, body=None):
        """create schema in database with database_id
        """
        body = body or {}
        database_id = int(database_id)
        jsonutils.schema_validate(body, self.CREATESCHEMA)
        auth = body.pop('auth', None)
        options = body.pop('options', None)
        schema = body.pop('schema', None)
        self._validate_schema(schema)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.create_schema(database_id, schema, auth, options, **kwargs)
        return resultutils.results(result='create empty schema success', data=[dbresult, ])

    def show(self, req, database_id, schema, body=None):
        body = body or {}
        database_id = int(database_id)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.show_schema(database_id, schema, **kwargs)
        return resultutils.results(result='show schema success', data=[dbresult, ])

    def update(self, req, database_id, schema, body=None):
        raise NotImplementedError

    def delete(self, req, database_id, schema, body=None):
        body = body or {}
        database_id = int(database_id)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        LOG.info('Try delete schema %s from %d' % (schema, database_id))
        dbresult = dbmanager.delete_schema(database_id, schema, **kwargs)
        return resultutils.results(result='delete schema success', data=[dbresult, ])

    def copy(self, req, database_id, schema, body=None):
        body = body or {}
        database_id = int(database_id)
        target_database_id = body.pop('target.database_id')
        target_schema = body.pop('target.schema')
        auth = body.pop('auth')
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.copy_schema(database_id, schema,
                                         target_database_id, target_schema,
                                         auth, **kwargs)
        return resultutils.results(result='copy schema from %d.%s success' % (database_id, schema),
                                   data=[dbresult, ])

    def bond(self, req, database_id, schema, body=None):
        """schema quote"""
        body = body or {}
        database_id = int(database_id)
        slave = body.get('slave', True)
        slave_id = body.get('slave_id')
        desc = body.get('desc')
        esure = body.get('esure', True)
        quote_id = body.get('quote_id')
        entity = body.pop('entity', None)
        entity = int(entity) if entity is not None else None
        endpoint = body.pop(common.ENDPOINTKEY, None)
        if esure:
            if not endpoint or not entity:
                raise InvalidArgument('No endpoint info or entity, esure should be flase')
            # TODO log entity info
            entity_info = entity_controller.show(req=req, endpoint=endpoint, entity=entity)['data'][0]
        session = endpoint_session()
        query = model_query(session, GopDatabase, filter=and_(GopDatabase.database_id == database_id,
                                                              GopDatabase.slave == 0))
        query = query.options(joinedload(GopDatabase.schemas, innerjoin=False))
        _database = query.one()
        _schema = None
        for __schema in _database.schemas:
            if __schema.schema == schema:
                _schema = __schema
                break
        if not _schema:
            raise exceptions.AcceptableSchemaError('Schema %s not found' % schema)
        quote_database_id = _database.database_id
        user = _schema.user
        passwd = _schema.passwd
        # glock = get_global().lock('entitys')
        # with glock(common.DB, [entity, ]):
        if slave:
            slaves = [_slave.slave_id for _slave in _database.slaves if _slave.ready]
            if slave_id:
                if slave_id not in slaves:
                    raise exceptions.AcceptableDbError('Slave %d not found or not ready' % slave)
                quote_database_id = slave_id
            else:
                if slaves:
                    quote_database_id = slaves[0]
                else:
                    LOG.warning('Not slave database, use master database as slave')
            user = _schema.ro_user
            passwd = _schema.ro_passwd
        address = _address([quote_database_id, ]).get(quote_database_id)
        with session.begin():
            schema_quote = SchemaQuote(quote_id=quote_id,
                                       schema_id=_schema.schema_id,
                                       qdatabase_id=quote_database_id,
                                       entity=entity, endpoint=endpoint, desc=desc)
            session.add(schema_quote)
            session.flush()
        port = address.get('port')
        host = address.get('host')
        return resultutils.results(result='quote to %s.%d success' % (schema_quote.qdatabase_id,
                                                                      schema_quote.schema_id),
                                   data=[dict(schema_id=schema_quote.schema_id,
                                              quote_id=schema_quote.quote_id,
                                              qdatabase_id=quote_database_id,
                                              host=host,
                                              port=port,
                                              user=user,
                                              passwd=passwd,
                                              schema=schema)])

    def phpadmin(self, req, database_id, schema, body=None):
        body = body or {}
        slave = body.get('slave', True)
        name = body.get('name') or ('unkonwn %s' % 'slave' if slave else 'master')
        session = endpoint_session(readonly=True)
        query = model_query(session, GopDatabase, filter=and_(GopDatabase.database_id == database_id,
                                                              GopDatabase.slave == 0))
        query = query.options(joinedload(GopDatabase.schemas, innerjoin=False))
        _database = query.one()
        _schema = None
        for __schema in _database.schemas:
            if __schema.schema == schema:
                _schema = __schema
                break
        if not _schema:
            raise InvalidArgument('Schema %s not found' % schema)
        target = _database.database_id
        user = _schema.user
        passwd = _schema.passwd
        if slave:
            slaves = [_slave.slave_id for _slave in _database.slaves if _slave.ready]
            if slaves:
                target = slaves[0]
            else:
                LOG.warning('Not slave database, use master database as slave')
            user = _schema.ro_user
            passwd = _schema.ro_passwd
        address = _address([target, ]).get(target)
        port = address.get('port')
        host = address.get('host')
        return cache_controller.create(req, body=dict(host=host, port=port, user=user, passwd=passwd,
                                                      schema=schema,
                                                      client=req.client_addr, name=name))

    def unquote(self, req, quote_id, body=None):
        """schema unquote"""
        body = body or {}
        session = endpoint_session()
        query = model_query(session, SchemaQuote, filter=SchemaQuote.quote_id == quote_id)
        with session.begin():
            schema_quote = query.one()
            query.delete()
        return resultutils.results(result='unquote from %s.%d success' % (schema_quote.qdatabase_id,
                                                                          schema_quote.schema_id,),
                                   data=[dict(quote_id=schema_quote.quote_id,
                                              schema_id=schema_quote.schema_id,
                                              qdatabase_id=schema_quote.qdatabase_id,
                                              entity=schema_quote.entity,
                                              endpoint=schema_quote.endpoint)])

    def quote(self, req, quote_id, body=None):
        """show schema quote info"""
        body = body or {}
        session = endpoint_session(readonly=True)
        query = model_query(session, SchemaQuote, filter=SchemaQuote.quote_id == quote_id)
        schema_quote = query.one()
        data = dict(quote_id=quote_id,
                    schema_id=schema_quote.schema_id,
                    qdatabase_id=schema_quote.qdatabase_id,
                    entity=schema_quote.entity,
                    endpoint=schema_quote.endpoint,
                    )
        if body.get('schema', False):
            schema = schema_quote.schema
            data.setdefault('schema', dict(schema=schema.schema,
                                           character_set=schema.character_set,
                                           collation_type=schema.collation_type))
        if body.get('database', False):
            database = schema_quote.database
            data.setdefault('database', dict(impl=database.impl,
                                             reflection_id=database.reflection_id,
                                             slave=database.slave,
                                             dbtype=database.dbtype,
                                             dbversion=database.dbversion))
        return resultutils.results(result='get quote success', data=[data, ])

    def quotes(self, req, body=None):
        body = body or {}
        session = endpoint_session(readonly=True)
        endpoint = body.pop('endpoint')
        entitys = argutils.map_to_int(body.pop('entitys'))
        if len(entitys) > 5:
            raise InvalidArgument('This api can not get entitys more then 5')
        query = session.query(SchemaQuote.quote_id,
                              SchemaQuote.schema_id,
                              SchemaQuote.qdatabase_id,
                              SchemaQuote.entity,
                              SchemaQuote.endpoint,
                              GopSchema.database_id,
                              GopSchema.schema,
                              GopSchema.user,
                              GopSchema.passwd,
                              GopSchema.ro_user,
                              GopSchema.ro_passwd,
                              GopSchema.character_set,
                              GopSchema.collation_type,
                              ).join(GopSchema, and_(GopSchema.schema_id == SchemaQuote.schema_id))
        query = query.filter(and_(SchemaQuote.endpoint == endpoint,
                                  SchemaQuote.entity.in_(entitys)))
        quotes = []
        database_ids = set()
        for quote in query:
            database_ids.add(quote.qdatabase_id)
            quotes.append(dict(quote_id=quote.quote_id,
                               schema_id=quote.schema_id,
                               qdatabase_id=quote.qdatabase_id,
                               database_id=quote.database_id,
                               schema=quote.schema,
                               user=quote.user,
                               passwd=quote.passwd,
                               ro_user=quote.ro_user,
                               ro_passwd=quote.ro_passwd,
                               character_set=quote.character_set,
                               collation_type=quote.collation_type))
        # get all address
        alladdress = _address(database_ids)
        for quote in quotes:
            quote.update(alladdress[quote.get('qdatabase_id')])

        return resultutils.results(result='list quotes success', data=quotes)
