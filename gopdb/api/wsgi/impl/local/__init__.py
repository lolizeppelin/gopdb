# -*- coding:utf-8 -*-
import contextlib
import eventlet

from sqlalchemy.pool import NullPool

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import argutils
from simpleutil.utils import uuidutils

from simpleservice.ormdb.argformater import connformater
from simpleservice.ormdb.engines import create_engine
from simpleservice.ormdb.tools import utils

from goperation.manager import common as manager_common
from goperation.manager.api import get_client
from goperation.manager.api import rpcfinishtime
from goperation.manager.utils import targetutils
from goperation.manager.wsgi.entity.controller import EntityReuest
from goperation.manager.wsgi.exceptions import RpcResultError

from gopdb import common
from gopdb.api.wsgi import exceptions
from gopdb.api.wsgi.impl import DatabaseManagerBase
from gopdb.api.wsgi.impl import privilegeutils
from gopdb.models import GopDatabase


LOG = logging.getLogger(__name__)

entity_controller = EntityReuest()


class DatabaseManager(DatabaseManagerBase):

    def _get_entity(self, req, entity):
        _entity = entity_controller.show(req=req, entity=entity,
                                         endpoint=common.DB, body={'ports': True})['data'][0]
        port = _entity['ports'][0] if _entity['ports'] else -1
        metadata = _entity['metadata']
        if not metadata:
            local_ip = 'unkonwn'
        else:
            local_ip = metadata.get('local_ip')
        return local_ip, port

    def _select_database(self, session, query, dbtype, **kwargs):

        disk = kwargs.pop('disk', 2000)
        free = kwargs.pop('memory', 1000)
        zone = kwargs.pop('zone', 'all')
        cpu = kwargs.pop('cpu', 2)
        # 包含规则
        includes = ['metadata.zone=%s' % zone,
                    'metadata.agent_type=application',
                    'metadata.%s!=None' % dbtype,
                    'metadata.%s>=5.5' % dbtype,
                    'disk>=%d' % disk, 'free>=%d' % free, 'cpu>=%d' % cpu]
        # 排序规则
        weighters = [{'iowait': 3},
                     {'cputime': 5},
                     {'free': 200},
                     {'cpu': -1},
                     {'left': -300},
                     {'process': None}]
        result = []

        def _chioces():
            return entity_controller.chioces(common.DB, includes, weighters)
        # 异步获取符合条件的agents排序
        chioces = eventlet.spawn(_chioces)
        entitys = set()
        query = query.filter_by(impl='local')
        # 亲和性字典
        affinitys = {}
        # 查询数据库,按照不同亲和性放置到亲和性字典
        for _database in query:
            entitys.add(int(_database.reflection_id))
            try:
                affinitys[_database.affinity].append(_database)
            except KeyError:
                affinitys[_database.affinity] = [_database]
        if not affinitys:
            LOG.info('No local database found')
            return result

        agents = chioces.wait()
        if not agents:
            LOG.info('No agent found for local database ')
            return result
        # agent排序结果放入字典中方便后面调用
        _agents = {}
        for index, agent_id in enumerate(agents):
            _agents[agent_id] = index
        emaps = entity_controller.shows(common.DB, entitys=entitys,
                                        ports=False, metadata=False)

        def _weight(database):
            # 排序的key列表
            sortkeys = []
            try:
                # 按照agent的排序结果
                entityinfo = emaps.get(int(database.reflection_id))
                sortkeys.append(_agents[entityinfo.get('agent_id')])
            except KeyError:
                raise InvalidArgument('No local agents found for entity %s' % database.reflection_id)
            # 按照schemas数量
            sortkeys.append(len(database.schemas))

        for affinity in affinitys:
            result.append(dict(affinity=affinity,
                               databases=[_database.database_id
                                          # 数据库按照agent性能排序规则排序
                                          for _database in sorted(affinitys[affinity], key=_weight)]
                               ))
        return result

    @contextlib.contextmanager
    def _reflect_database(self, session, **kwargs):
        """impl reflect code"""
        entitys = kwargs.get('entitys', None)
        if entitys:
            entitys = argutils.map_with(entitys, str)
            _filter = GopDatabase.reflection_id.in_(entitys)
        else:
            _filter = None
        yield 'entity', _filter

    @contextlib.contextmanager
    def _show_database(self, session, database, **kwargs):
        """show database info"""
        req = kwargs.pop('req')
        yield self._get_entity(req, int(database.reflection_id))

    @contextlib.contextmanager
    def _create_database(self, session, database, **kwargs):
        req = kwargs.pop('req')
        agent_id = kwargs.get('agent_id')
        if not agent_id:
            zone = kwargs.pop('zone', 'all')
            if not zone:
                raise InvalidArgument('Auto select database agent need zone')
            includes = ['metadata.zone=%s' % zone,
                        'metadata.agent_type=application',
                        'disk>=500', 'free>=200']
            weighters = [
                {'iowait': 3},
                {'free': 200},
                {'left': 500},
                {'cputime': 5},
                {'cpu': -1},
                {'process': None}]
            chioces = entity_controller.chioces(common.DB, includes=includes, weighters=weighters)
            if chioces:
                agent_id = chioces[0]
            else:
                raise InvalidArgument('Not agent found for %s' % common.DB)
        body = dict(dbtype=database.dbtype,
                    auth=dict(user=database.user, passwd=database.passwd))
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
                                             endpoint=common.DB, database_id=database.database_id,
                                             dbtype=database.dbtype)

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

    def _start_database(self, database, **kwargs):
        req = kwargs.pop('req')
        entity = int(database.reflection_id)
        _entity = entity_controller.show(req=req, entity=entity,
                                         endpoint=common.DB, body={'ports': False})['data'][0]
        agent_id = _entity['agent_id']
        metadata = _entity['metadata']
        target = targetutils.target_agent_by_string(metadata.get('agent_type'),
                                                    metadata.get('host'))
        target.namespace = common.DB
        rpc = get_client()
        finishtime, timeout = rpcfinishtime()
        rpc_ret = rpc.call(target, ctxt={'finishtime': finishtime,
                                         'agents': [agent_id, ]},
                           msg={'method': 'start_entity', 'args': dict(entity=entity)},
                           timeout=timeout)
        if not rpc_ret:
            raise RpcResultError('create entitys result is None')
        if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
            raise RpcResultError('create entity fail %s' % rpc_ret.get('result'))
        return rpc_ret

    def _stop_database(self, database, **kwargs):
        req = kwargs.pop('req')
        entity = int(database.reflection_id)
        _entity = entity_controller.show(req=req, entity=entity,
                                         endpoint=common.DB, body={'ports': False})['data'][0]
        agent_id = _entity['agent_id']
        metadata = _entity['metadata']
        target = targetutils.target_agent_by_string(metadata.get('agent_type'),
                                                    metadata.get('host'))
        target.namespace = common.DB
        rpc = get_client()
        finishtime, timeout = rpcfinishtime()
        rpc_ret = rpc.call(target, ctxt={'finishtime': finishtime,
                                         'agents': [agent_id, ]},
                           msg={'method': 'stop_entity',
                                'args': dict(entity=entity)},
                           timeout=timeout)
        if not rpc_ret:
            raise RpcResultError('stop database entity result is None')
        if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
            raise RpcResultError('stop database entity fail %s' % rpc_ret.get('result'))
        return rpc_ret

    def _status_database(self, database, **kwargs):
        req = kwargs.pop('req')
        entity = int(database.reflection_id)
        _entity = entity_controller.show(req=req, entity=entity,
                                         endpoint=common.DB, body={'ports': False})['data'][0]
        agent_id = _entity['agent_id']
        metadata = _entity['metadata']
        target = targetutils.target_agent_by_string(metadata.get('agent_type'),
                                                    metadata.get('host'))
        target.namespace = common.DB
        rpc = get_client()
        finishtime, timeout = rpcfinishtime()
        rpc_ret = rpc.call(target, ctxt={'finishtime': finishtime,
                                         'agents': [agent_id, ]},
                           msg={'method': 'status_entity',
                                'args': dict(entity=entity)},
                           timeout=timeout)
        if not rpc_ret:
            raise RpcResultError('status database entity result is None')
        if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
            raise RpcResultError('status database entity fail %s' % rpc_ret.get('result'))
        return rpc_ret

    def _address(self, session, dbmaps):
        entitys = map(int, dbmaps.keys())
        emaps = entity_controller.shows(endpoint=common.DB, entitys=entitys)
        address_maps = dict()
        for entity in emaps:
            entityinfo = emaps[entity]
            port = entityinfo['ports'][0] if entityinfo['ports'] else -1
            host = entityinfo['metadata']['local_ip'] if entityinfo['metadata'] else None
            address_maps[dbmaps[str(entity)]] = dict(host=host, port=port)
        return address_maps

    @contextlib.contextmanager
    def _show_schema(self, session, database, schema, **kwargs):
        req = kwargs.pop('req')
        yield self._get_entity(req, int(database.reflection_id))

    @contextlib.contextmanager
    def _create_schema(self, session,
                       database, schema, auths, options, **kwargs):
        """create new schema intance on database_id"""
        req = kwargs.pop('req')
        try:
            local_ip, port = self._get_entity(req, int(database.reflection_id))
            connection = connformater % dict(user=database.user, passwd=database.passwd,
                                             host=local_ip, port=port, schema=schema)
            engine = create_engine(connection, thread_checkin=False, poolclass=NullPool)
            utils.create_schema(engine, auths=auths,
                                character_set=options.get('character_set'),
                                collation_type=options.get('collation_type'),
                                connection_timeout=3)
            yield local_ip, port
        except Exception:
            LOG.exception('Create schema fail')
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
        if port <= 0:
            raise exceptions.AcceptableDbError('Can not find Database port, not init finished')
        if not local_ip:
            raise exceptions.AcceptableDbError('Database agent is offline now')
        engine = create_engine(connformater % dict(user=database.user, passwd=database.passwd,
                                                   schema=schema.schema, host=local_ip, port=port),
                               thread_checkin=False, poolclass=NullPool)
        dropauths = None
        if schema.user != database.user:
            dropauths = privilegeutils.mysql_privileges(schema)
        utils.drop_schema(engine, dropauths)
        yield local_ip, port

    def _create_slave_database(self, *args, **kwargs):
        raise NotImplementedError
