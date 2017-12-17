import contextlib
from sqlalchemy.pool import NullPool

from simpleutil.utils import uuidutils
from simpleservice.ormdb.argformater import connformater
from simpleservice.ormdb.engines import create_engine
from simpleservice.ormdb.tools import utils

from goperation.manager.wsgi.entity.controller import EntityReuest

from gopdb import common
from gopdb.api.wsgi.impl import DatabaseManagerBase
from gopdb.api.wsgi.impl import exceptions
from gopdb.api.wsgi.impl import privilegeutils

entity_controller = EntityReuest()


class DataBaseManager(DatabaseManagerBase):

    def _get_entity(self, req, entity):
        _entity = entity_controller.show(req=req, entity=entity,
                                         endpoint=common.DB, body={'ports': True})['data'][0]
        port = _entity['ports'][0] if _entity['ports'] else -1
        agent_id = _entity['agent_id']
        agent_attributes = entity_controller.agent_attributes(agent_id)
        if not agent_attributes:
            raise exceptions.AcceptableDbError('Agent %d not online or not exist' % agent_id)
        yield agent_attributes.get('local_ip'), port

    @contextlib.contextmanager
    def _show_database(self, session, database, **kwargs):
        """show database info"""
        req = kwargs.pop('req')
        yield self._get_entity(req, int(database.reflection_id))

    @contextlib.contextmanager
    def _create_database(self, session, database, **kwargs):
        req = kwargs.pop('req')
        agent_id = kwargs.pop('agent_id')
        body = dict(dbtype=database.dbtype,
                    user=database.user, passwd=database.passwd)
        body.update(kwargs)
        entity = entity_controller.create(req=req,
                                          agent_id=agent_id,
                                          endpoint=common.DB,
                                          body=body)['data'][0]['entity']
        database.impl = 'local'
        database.status = common.UNACTIVE
        database.reflection_id = str(entity)
        yield self._get_entity(req=req, entity=entity)

    def _esure_create(self, database, **kwargs):
        entity_controller.post_create_entity(entity=int(database.reflection_id),
                                             endpoint=common.DB, database_id=database.database_id)

    @contextlib.contextmanager
    def _delete_database(self, session, database, **kwargs):
        req = kwargs.pop('req')
        local_ip, port = self._get_entity(req=req, entity=int(database.reflection_id))
        token = uuidutils.generate_uuid()
        entity_controller.delete(req=req, endpoint=common.DB, entity=int(database.reflection_id),
                                 body=dict(token=token))
        yield local_ip, port

    @contextlib.contextmanager
    def _delete_slave_database(self, session, slave, masters, **kwargs):
        req = kwargs.pop('req')
        local_ip, port = self._get_entity(req, int(slave.reflection_id))
        entity_controller.delete(req=req, endpoint=common.DB, entity=int(slave.reflection_id))
        try:
            yield local_ip, port
        except Exception:
            raise
        else:
            for master in masters:
                master_ip, master_port = self._get_entity(req, int(master.reflection_id))
                privilegeutils.mysql_drop_replprivileges(master, slave, master_ip, master_port)

    @contextlib.contextmanager
    def _show_schema(self, session, database, schema, **kwargs):
        req = kwargs.pop('req')
        yield self._get_entity(req, int(database.reflection_id))

    @contextlib.contextmanager
    def _create_schema(self, session,
                       database, schema, auths, options, **kwargs):
        """create new schema intance on database_id"""
        req = kwargs.pop('req')
        engine = None
        try:
            local_ip, port = self._get_entity(req, int(database.reflection_id))
            connection = connformater % dict(user=database.user, passwd=database.passwd,
                                             host=local_ip, port=port)
            _engine = create_engine(connection, thread_checkin=False, poolclass=NullPool)
            utils.create_schema(_engine, auths=auths,
                                charcter_set=options.get('charcter_set'),
                                collation_type=options.get('collation_type'))
            engine = _engine
            yield local_ip, port
        except Exception:
            if engine:
                utils.drop_schema(engine, auths)
            raise

    @contextlib.contextmanager
    def _copy_schema(self, session,
                     src_database, src_schema,
                     dst_database, dst_schema,
                     auths, **kwargs):
        req = kwargs.pop('req')
        src_port, src_local_ip = self._get_entity(req, int(src_database.reflection_id))
        dst_port, dst_local_ip = self._get_entity(req, int(dst_database.reflection_id))
        src_info = dict(user=src_database.user, passwd=src_database.passwd,
                        host=src_local_ip, port=src_port)
        dst_info = dict(user=dst_database.user, passwd=dst_database.passwd,
                        host=dst_local_ip, port=dst_port)
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
        req = kwargs.pop('req')
        local_ip, port = self._get_entity(req, int(database.reflection_id))
        engine = create_engine(connformater % dict(user=database.user, passwd=database.passwd,
                                                   schema=schema, host=local_ip, port=port),
                               thread_checkin=False, poolclass=NullPool)
        dropauths = None
        if schema.user != database.user:
            dropauths = privilegeutils.mysql_privileges(schema)
        utils.drop_schema(engine, dropauths)
        yield local_ip, port

    def _create_slave_database(self, *args, **kwargs):
        raise NotImplementedError
