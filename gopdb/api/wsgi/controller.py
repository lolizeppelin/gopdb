import re
import webob.exc

from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import jsonutils
from simpleutil.utils import singleton
from simpleutil.utils import argutils

from simpleservice.ormdb.api import model_query
from simpleservice.rpc.exceptions import AMQPDestinationNotFound
from simpleservice.rpc.exceptions import MessagingTimeout
from simpleservice.rpc.exceptions import NoSuchMethod

from goperation.manager.exceptions import CacheStoneError
from goperation.manager.utils import resultutils
from goperation.manager.wsgi.contorller import BaseContorller
from goperation.manager.wsgi.entity.controller import EntityReuest
from goperation.manager.wsgi.endpoint.controller import EndpointReuest
from goperation.manager.wsgi.exceptions import RpcPrepareError
from goperation.manager.wsgi.exceptions import RpcResultError

from gopdb import common
from gopdb import utils
from gopdb.api import endpoint_session
from gopdb.api.wsgi.impl import exceptions
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
             MultipleResultsFound: webob.exc.HTTPInternalServerError
             }

MANAGERCACHE = {}

entity_controller = EntityReuest()
endpoint_controller = EndpointReuest()


def _impl(database_id):
    try:
        return MANAGERCACHE[database_id]
    except KeyError:
        session = endpoint_session(readonly=True)
        try:
            database = model_query(session, GopDatabase, GopDatabase.database_id == database_id).one()
            if database_id not in MANAGERCACHE:
                dbmanager = utils.impl_cls('wsgi', database.impl)
                MANAGERCACHE.setdefault(database_id, dbmanager)
            return MANAGERCACHE[database_id]
        finally:
            session.close()


@singleton.singleton
class DatabaseReuest(BaseContorller):

    def reflect(self, req, impl, body=None):
        body = body or {}
        dbmanager = utils.impl_cls('wsgi', impl)
        reflect_list = dbmanager.reflect_database(**body)
        return resultutils.results(result='reflect database success', data=reflect_list)

    def select(self, req, impl, body=None):
        body = body or {}
        dbmanager = utils.impl_cls('wsgi', impl)
        dbresult = dbmanager.select_database(**body)
        return resultutils.results(result='create database success', data=dbresult)

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
                 GopDatabase.is_master,
                 GopDatabase.impl,
                 GopDatabase.dbtype,
                 GopDatabase.dbversion,
                 GopDatabase.reflection_id,
                 GopDatabase.status,
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
                column['slaves'].append(dict(database_id=slave.package_id, readonly=slave.slave))
        return results

    def create(self, req, body=None):
        body = body or {}
        try:
            impl = body.pop('impl')
            dbtype = body.pop('dbtype')
            user = body.pop('user')
            passwd = body.pop('passwd')
            dbversion = body.pop('dbversion', None)
        except KeyError as e:
            raise InvalidArgument('miss key: %s' % e.message)
        affinity = body.pop('affinity', 0)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = utils.impl_cls('wsgi', impl)
        dbresult = dbmanager.create_database(user, passwd, dbtype, dbversion, affinity, **kwargs)
        return resultutils.results(result='create database success', data=[dbresult, ])

    def show(self, req, database_id, body=None):
        body = body or {}
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.show_database(database_id, **kwargs)
        return resultutils.results(result='show database success', data=[dbresult, ])

    def update(self, req, database_id, body=None):
        body = body or {}
        session = endpoint_session()
        query = model_query(session, GopDatabase, filter=GopDatabase.database_id == database_id)
        with session.begin():
            updata = {'status': body.get('status', common.UNACTIVE)}
            if body.get('dbversion'):
                updata.setdefault('dbversion', body.get('dbversion'))
            count = query.update(updata)
            if not count:
                LOG.warning('Update not match, no database has been updated')
        return resultutils.results(result='Update %s database success' % database_id)

    def delete(self, req, database_id, body=None):
        body = body or {}
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.delete_database(database_id, **kwargs)
        return resultutils.results(result='delete database success', data=[dbresult, ])

    def start(self, req, database_id, body=None):
        body = body or {}
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.start_database(database_id, **kwargs)
        return resultutils.results(result='start database success', data=[dbresult, ])

    def stop(self, req, database_id, body=None):
        body = body or {}
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.stop_database(database_id, **kwargs)
        return resultutils.results(result='stop database success', data=[dbresult, ])

    def status(self, req, database_id, body=None):
        body = body or {}
        raise NotImplementedError


@singleton.singleton
class SchemaReuest(BaseContorller):

    AUTHSCHEMA = {
        'type': 'object',
        'required': ['user', 'passwd', 'ro_user', 'ro_passwd', 'source'],
        'properties':
            {
                'user': {'type': 'string'},
                'passwd': {'type': 'string'},
                'ro_user': {'type': 'string'},
                'ro_passwd': {'type': 'string'},
                'source': {'type': 'string'},
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
                                                    GopSchema.charcter_set,
                                                    GopSchema.collation_type,
                                                    ],
                                           counter=GopSchema.schema_id,
                                           order=order, desc=desc,
                                           page_num=page_num)
        return results

    def create(self, req, database_id, body=None):
        """create schema in database with database_id
        """
        body = body or {}
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
        secret = body.get('secret', False)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        dbresult = dbmanager.show_schema(database_id, schema, secret, **kwargs)
        return resultutils.results(result='show schema success', data=[dbresult, ])

    def update(self, req, database_id, schema, body=None):
        raise NotImplementedError

    def delete(self, req, database_id, schema, body=None):
        body = body or {}
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = _impl(database_id)
        LOG.info('Try delete schema %s from %d' % (schema, database_id))
        dbresult = dbmanager.delete_schema(database_id, schema, **kwargs)
        return resultutils.results(result='delete schema success', data=[dbresult, ])

    def copy(self, req, database_id, schema, body=None):
        body = body or {}
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
        slave = body.get('slave')
        desc = body.get('desc')
        esure = body.get('esure', True)
        entity = int(body.pop('entity'))
        quote_id = body.get('quote_id')
        endpoint = body.pop(common.ENDPOINTKEY)
        if esure:
            # TODO log entity info
            entity_info = entity_controller.show(req=req, endpoint=endpoint, entity=entity)['data'][0]
        session = endpoint_session()
        query = model_query(session, GopDatabase, filter=and_(GopDatabase.database_id == database_id,
                                                              GopDatabase.is_master == True))
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
        # glock = get_global().lock('entitys')
        # with glock(common.DB, [entity, ]):
        with session.begin():
            if slave is not None:
                if slave > 0:
                    slaves = [_slave.slave_id for _slave in _database.slaves]
                    if slave not in slaves:
                        raise exceptions.AcceptableDbError('Slave %d not found' % slave)
                    quote_database_id = slave
                else:
                    # TODO auto select slave database
                    quote_database_id = _database.slaves[0]
            schema_quote = SchemaQuote(quote_id=quote_id,
                                       schema_id=_schema.schema_id,
                                       qdatabase_id=quote_database_id,
                                       entity=entity, endpoint=endpoint, desc=desc)
            session.add(schema_quote)
            session.flush()
        return resultutils.results(result='quote to %s.%d success' % (schema_quote.database_id,
                                                                      schema_quote.schema_id),
                                   data=[dict(schema_id=schema_quote.schema_id,
                                              quote_id=schema_quote.quote_id,
                                              qdatabase_id=database_id)])

    def unquote(self, req, quote_id, body=None):
        """schema unquote"""
        body = body or {}
        session = endpoint_session()
        query = model_query(session, SchemaQuote, filter=SchemaQuote.quote_id == quote_id)
        with session.begin():
            schema_quote = query.one()
            query.delete()
        return resultutils.results(result='unquote from %s.%d success' % (schema_quote.database_id,
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
                                           charcter_set=schema.charcter_set,
                                           collation_type=schema.collation_type))
        if body.get('database', False):
            database = schema_quote.database
            data.setdefault('database', dict(impl=database.impl,
                                             reflection_id=database.reflection_id,
                                             is_master=database.is_master,
                                             dbtype=database.dbtype,
                                             dbversion=database.dbversion))
        return resultutils.results(result='get quote success', data=[data, ])

    def quotes(self, req, body=None):
        body = body or {}
        session = endpoint_session(readonly=True)
        endpoint = body.pop('endpoint')
        entitys = argutils.map_to_int(body.pop('entitys'))

        query = session.query(SchemaQuote.quote_id,
                              SchemaQuote.schema_id,
                              SchemaQuote.qdatabase_id,
                              SchemaQuote.entity,
                              SchemaQuote.endpoint,
                              GopSchema.schema,
                              GopSchema.user,
                              GopSchema.passwd,
                              GopSchema.ro_user,
                              GopSchema.ro_passwd,
                              GopSchema.character_set,
                              GopSchema.collation_type,
                              ).join(GopSchema, and_(GopSchema.schema_id == SchemaQuote.schema_id))
        query.filter(and_(SchemaQuote.endpoint == endpoint,
                          SchemaQuote.entity.in_(entitys)))
        return resultutils.results(result='list quotes success', data=[dict(quote_id=quote.quote_id,
                                                                            schema_id=quote.schema_id,
                                                                            qdatabase_id=quote.qdatabase_id,
                                                                            database_id=quote.database_id,
                                                                            schema=quote.schema,
                                                                            user=quote.user,
                                                                            passwd=quote.passwd,
                                                                            ro_user=quote.ro_user,
                                                                            ro_passwd=quote.ro_passwd,
                                                                            character_set=quote.character_set,
                                                                            collation_type=quote.collation_type,
                                                                            ) for quote in query])
