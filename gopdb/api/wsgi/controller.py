import webob.exc

from sqlalchemy.sql import and_
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import jsonutils
from simpleutil.utils import timeutils
from simpleutil.utils import singleton
from simpleservice.ormdb.api import model_query
from simpleservice.rpc.exceptions import AMQPDestinationNotFound
from simpleservice.rpc.exceptions import MessagingTimeout
from simpleservice.rpc.exceptions import NoSuchMethod

from goperation.manager.api import get_global
from goperation.manager.utils import resultutils

from goperation.manager.exceptions import CacheStoneError

from goperation.manager.wsgi.contorller import BaseContorller
from goperation.manager.wsgi.exceptions import RpcPrepareError
from goperation.manager.wsgi.exceptions import RpcResultError


from gopdb import common
from gopdb import utils

from gopdb.api import endpoint_session

from gopdb.models import GopDatabase
from gopdb.models import SchemaQuote

from gopdb.impl import exceptions


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


@singleton.singleton
class DatabaseReuest(BaseContorller):

    LOGSCHEMA = {
        'type': 'object',
        'required': ['start', 'end', 'size_change', 'logfile', 'detail'],
        'properties':
            {
                'start': {'type': 'integer'},
                'end': {'type': 'integer'},
                'size_change': {'type': 'integer'},
                'logfile': {'type': 'string'},
                'detail': {'oneOf': [{'type': 'object'},
                                     {'type': 'null'}],
                           'description': 'detail of request'},
            }
    }

    # TODO cache dbmanager of database_id
    def _impl(self, database_id):
        session = endpoint_session(readonly=True)
        database = model_query(session, GopDatabase, GopDatabase.database_id == database_id).one()
        dbmanager = utils.impl_cls(database.impl)
        session.close()
        return dbmanager

    def select(self, req, body=None):
        body = body or {}
        raise NotImplementedError

    def index(self, req, body=None):
        body = body or {}
        order = body.pop('order', None)
        desc = body.pop('desc', False)
        page_num = int(body.pop('page_num', 0))

        slaves = body.pop('slaves', False)
        schemas = body.pop('schemas', False)
        quotes = body.pop('quotes', False)
        impl = body.get('quotes', None)
        session = endpoint_session(readonly=True)
        _filter = None
        if impl:
            _filter = GopDatabase.impl == impl

        columns=[GopDatabase.database_id,
                 GopDatabase.is_master,
                 GopDatabase.impl,
                 GopDatabase.reflection_id,
                 GopDatabase.status,
                 GopDatabase.desc]

        options = []
        if slaves:
            columns.append(GopDatabase.slaves)
            options.append(joinedload(GopDatabase.slaves, innerjoin=False))
        if schemas:
            columns.append(GopDatabase.schemas)
            options.append(joinedload(GopDatabase.schemas, innerjoin=False))
        if quotes:
            columns.append(GopDatabase.quotes)
            options.append(joinedload(GopDatabase.quotes, innerjoin=False))

        results = resultutils.bulk_results(session,
                                           model=GopDatabase,
                                           columns=[GopDatabase.database_id,
                                                    GopDatabase.is_master,
                                                    GopDatabase.impl,
                                                    GopDatabase.reflection_id,
                                                    GopDatabase.status,
                                                    GopDatabase.desc,
                                                    GopDatabase.slaves,
                                                    GopDatabase.schemas,
                                                    GopDatabase.quotes],
                                           counter=GopDatabase.database_id,
                                           order=order, desc=desc,
                                           option=options,
                                           filter=_filter,
                                           page_num=page_num)
        for column in results['data']:

            slaves = column.get('slaves', [])
            column['slaves'] = []
            for slave in slaves:
                column['slaves'].append(dict(database_id=slave.package_id, readonly=slave.slave))

            schemas = column.get('schemas', [])
            column['schemas'] = []
            for schema in schemas:
                column['schemas'].append(dict(schema_id=schema.schema_id,
                                              schema=schema.schema,
                                              charcter_set=schema.charcter_set,
                                              collation_type=schema.collation_type,
                                              desc=schema.desc))

            quotes = column.get('quotes', [])
            column['quotes'] = []
            for quote in quotes:
                column['quotes'].append(dict(quote_id=quote.quote_id,
                                             schema_id=quote.schema_id,
                                             entity=quote.entity,
                                             endpoint=quote.endpoint,
                                             desc=quote.desc))

        return results

    def create(self, req, body=None):
        body = body or {}
        impl = body.pop('impl')
        user = body.pop('user')
        passwd = body.pop('passwd')
        affinity = body.pop('affinity', 0)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = utils.impl_cls(impl)
        dbresult = dbmanager.create_database(user, passwd, affinity, **kwargs)
        return resultutils.results(data=[dbresult, ])

    def show(self, req, database_id, body=None):
        body = body or {}
        master = body.get('master', True)
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = self._impl(database_id)
        dbresult = dbmanager.show_database(database_id, master, **kwargs)
        return resultutils.results(data=[dbresult, ])

    def update(self, req, database_id, body=None):
        body = body or {}
        session = endpoint_session()
        query = model_query(session, GopDatabase, filter=GopDatabase.database_id == database_id)
        with session.begin():
            count = query.update({'status': body.get('status', common.UNACTIVE)})
        return resultutils.results(result='Update %s database success' % database_id)

    def delete(self, req, database_id, body=None):
        body = body or {}
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = self._impl(database_id)
        dbresult = dbmanager.delete_database(database_id, **kwargs)
        return resultutils.results(data=[dbresult, ])

    def start(self, req, database_id, body=None):
        body = body or {}
        raise NotImplementedError

    def stop(self, req, database_id, body=None):
        body = body or {}
        raise NotImplementedError

    def status(self, req, database_id, body=None):
        body = body or {}
        raise NotImplementedError


@singleton.singleton
class SchemaReuest(BaseContorller):

    def index(self, req, database_id, body=None):
        pass

    def create(self, req, database_id, body=None):
        auth = body.pop('auth')
        options = body.pop('options')
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = self._impl(database_id)
        dbresult = dbmanager.create_schema(database_id, auth, options, **kwargs)
        resultutils.results(result='create empty schema success', data=[dbresult, ])

    def show(self, req, database_id, schema, body=None):
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = self._impl(database_id)
        dbresult = dbmanager.show_schema(database_id, schema, **kwargs)
        resultutils.results(result='show schema success', data=[dbresult, ])

    def update(self, req, database_id, schema, body=None):
        raise NotImplementedError

    def delete(self, req, database_id, schema, body=None):
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = self._impl(database_id)
        dbresult = dbmanager.delete_schema(database_id, schema, **kwargs)
        resultutils.results(result='delete schema success', data=[dbresult, ])

    def copy(self, req, database_id, schema, body=None):
        target_database_id = body.pop('target.database_id')
        target_schema = body.pop('target.schema')
        auth = body.pop('auth')
        kwargs = dict(req=req)
        kwargs.update(body)
        dbmanager = self._impl(database_id)
        dbresult = dbmanager.copy_schema(database_id, schema,
                                         target_database_id, target_schema,
                                         auth, **kwargs)
        resultutils.results(result='copy schema from %d.%s success' % (database_id, schema),
                            data=[dbresult, ])

    def bond(self, req, database_id, schema, body=None):
        """schema quote"""
        body = body or {}
        slave = body.get('slave')
        desc = body.get('desc')
        entity = int(body.pop('entity'))
        endpoint = int(body.pop(common.ENDPOINTKEY))
        session = endpoint_session()
        query = model_query(session, GopDatabase, filter=and_(GopDatabase.database_id == database_id,
                                                              GopDatabase.is_master == True))
        if slave is not None:
            query = query.optione(joinedload(GopDatabase.slaves))
        query = query.optione(joinedload(GopDatabase.schemas))
        _database = query.one()
        _schema = None
        for __schema in _database.schemas:
            if __schema.schema == schema:
                _schema = __schema
                break
        if not _schema:
            raise exceptions.AcceptableSchemaError('Schema not found')
        quote_database_id = _database.database_id
        if slave is not None:
            if slave > 0:
                if slave not in _database.slaves:
                    raise exceptions.AcceptableDbError('Slave %d not found' % slave)
                quote_database_id = slave
            else:
                # TODO auto select database
                quote_database_id = _database.slaves[0]
        schema_quote = SchemaQuote(schema_id=_schema.schema_id,
                                   database_id=quote_database_id,
                                   entity=entity, endpoint=endpoint, desc=desc)
        # glock = get_global().lock('entitys')
        # with glock(common.DB, [entity, ]):
        with session.begin():
            session.add(schema_quote)
            session.flush()
        return resultutils.results(result='quote to %s.%d success' % (schema_quote.database_id,
                                                                      schema_quote.schema_id),
                                   data=[dict(schema_id=schema_quote.schema_id,
                                              quote_id=schema_quote.quote_id,
                                              database_id=database_id)])

    def unquote(self, req, quote_id, body=None):
        """schema unquote"""
        body = body or {}
        session = endpoint_session()
        query = model_query(session, SchemaQuote, filter=SchemaQuote.quote_id == quote_id)
        with session.begin():
            schema_quote = query.one()
            query.delete()
        return resultutils.results(result='unquote from %s.%d success' % (schema_quote.database_id,
                                                                          schema_quote.schema_id))

    def quote(self, req, quote_id, body=None):
        body = body or {}
        session = endpoint_session(readonly=True)
        query = model_query(session, SchemaQuote, filter=SchemaQuote.quote_id == quote_id)
        schema_quote = query.one()
        data = dict(quote_id=quote_id,
                    schema_id=schema_quote.schema_id,
                    database_id=schema_quote.database_id,
                    entity=schema_quote.entity,
                    endpoint=schema_quote.endpoint,
                    )
        if body.get('schema', False):
            schema = schema_quote.schema
            data.setdefault('schema', schema.schema)
            data.setdefault('charcter_set', schema.charcter_set)
            data.setdefault('collation_type', schema.collation_type)
            data.setdefault('desc', schema.desc)
        if body.get('database', False):
            database = schema_quote.database
            data.setdefault('impl', database.impl)
            data.setdefault('reflection_id', database.reflection_id)
            data.setdefault('is_master', database.is_master)
        return resultutils.results(result='get quote success', data=[data, ])
