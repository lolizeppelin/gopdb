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
from gopdb.models import GopSalveRelation


LOG = logging.getLogger(__name__)

entity_controller = EntityReuest()


class DatabaseManager(DatabaseManagerBase):
    # 排序规则
    weighters = [
        {'iowait': 3},
        {'cputime': 5},
        {'free': -200},
        {'cpu': -1},
        {'left': -300},
        {'metadata.gopdb-aff': None},
        {'process': None}
    ]

    def select_agents(self, dbtype, **kwargs):
        return self._select_agents(dbtype, **kwargs)

    @staticmethod
    def _select_agents(dbtype, **kwargs):
        disk = kwargs.pop('disk', 2000)
        free = kwargs.pop('memory', 1000)
        zone = kwargs.pop('zone', 'all')
        cpu = kwargs.pop('cpu', 2)
        affinity = 0
        if kwargs.pop('master', True):
            affinity = affinity | 1
        if kwargs.pop('slave', False):
            affinity = affinity | 2
        # 包含规则
        includes = ['metadata.zone=%s' % zone,
                    'metadata.agent_type=application',
                    'metadata.gopdb-aff&%d' % affinity,
                    'metadata.%s!=None' % dbtype,
                    'metadata.%s>=5.5' % dbtype,
                    'disk>=%d' % disk, 'free>=%d' % free, 'cpu>=%d' % cpu]
        return entity_controller.chioces(common.DB, includes, DatabaseManager.weighters)

    def _bond_slave(self, req,
                    master, master_host, master_port,
                    slave, slave_host, slave_port,
                    repl):
        if master_host == slave_host:
            raise exceptions.UnAcceptableDbError('Master and Salve in same host')
        try:
            # master do
            connection = connformater % dict(user=master.user, passwd=master.passwd,
                                             host=master_host, port=master_port, schema='')
            engine = create_engine(connection, thread_checkin=False, poolclass=NullPool)
            with engine.connect() as conn:
                LOG.info('Login master database to get pos and file')
                r = conn.execute('show master status')
                results = r.fetchall()
                r.close()
            if not results:
                raise exceptions.UnAcceptableDbError('Master bind log not open!')
            binlog = results[0]
            if binlog.get('file')[-1] != '1' or binlog.get('position') > 1000:
                raise exceptions.UnAcceptableDbError('Database pos of file error')
            # slave do
            slave_info = dict(replname='database-%d' % master.database_id,
                              host=master_host, port=master_port,
                              repluser=repl.get('user'), replpasswd=repl.get('passwd'),
                              file=binlog.get('file'), pos=binlog.get('position'))
            sqls = ['SHOW SLAVE STATUS']
            sqls.append("CHANGE MASTER '%(replname)s' TO MASTER_HOST='%(host)s', MASTER_PORT=%(port)d," \
                        "MASTER_USER='%(repluser)s',MASTER_PASSWORD='%(replpasswd)s'," \
                        "MASTER_LOG_FILE='%(file)s',MASTER_LOG_POS=%(pos)s)" % slave_info)
            sqls.append('START salve %(replname)s' % slave_info)

            connection = connformater % dict(user=slave.user, passwd=slave.passwd,
                                             host=slave_host, port=slave_port, schema='')
            engine = create_engine(connection, thread_checkin=False, poolclass=NullPool)
            with engine.connect() as conn:
                LOG.info('Login slave database for init')
                r = conn.execute(sqls[0])
                if LOG.isEnabledFor(logging.DEBUG):
                    for row in r.fetchall():
                        LOG.debug(str(row))
                r.close()
                r = conn.execute(sqls[1])
                r.close()
                LOG.debug('Success add repl info')
                try:
                    r = conn.execute(sqls[2])
                except Exception:
                    LOG.error('Start slave fail')
                    raise exceptions.UnAcceptableDbError('Start slave fail')
                else:
                    r.close()
        except Exception:
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.exception('Bond slave fail')
            raise exceptions.UnAcceptableDbError('Call slave start repl fail')

    def _get_entity(self, req, entity, raise_error=False):
        _entity = entity_controller.show(req=req, entity=entity,
                                         endpoint=common.DB, body={'ports': True})['data'][0]
        port = _entity['ports'][0] if _entity['ports'] else -1
        metadata = _entity['metadata']
        if not metadata:
            if raise_error:
                raise InvalidArgument('Target entity is offline')
            local_ip = 'unkonwn'
        else:
            local_ip = metadata.get('local_ip')
        if port < 0 and raise_error:
            raise InvalidArgument('Target entity no port now')
        return local_ip, port

    def _select_database(self, session, query, dbtype, **kwargs):

        result = []

        def _chioces():
            return self._select_agents(dbtype, **kwargs)
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
    def _create_database(self, session, database, bond, **kwargs):
        req = kwargs.pop('req')
        agent_id = kwargs.pop('agent_id', None)
        if database.slave:
            type_affinity = 2
        else:
            type_affinity = 1
        if not agent_id:
            zone = kwargs.pop('zone', 'all')
            if not zone:
                raise InvalidArgument('Auto select database agent need zone')
            includes = ['metadata.zone=%s' % zone,
                        'metadata.agent_type=application',
                        'metadata.gopdb-aff&%d' % type_affinity,
                        'disk>=500', 'free>=200']
            weighters = [
                {'iowait': 3},
                {'free': 200},
                {'left': 500},
                {'cputime': 5},
                {'cpu': -1},
                {'metadata.gopdb-aff': None},
                {'process': None}]
            chioces = entity_controller.chioces(common.DB, includes=includes, weighters=weighters)
            if chioces:
                agent_id = chioces[0]
                LOG.info('Auto select  database agent %d' % agent_id)
            else:
                raise InvalidArgument('Not agent found for %s' % common.DB)
        body = dict(dbtype=database.dbtype,
                    auth=dict(user=database.user, passwd=database.passwd))
        configs = kwargs.pop('configs', {})
        body.update(kwargs)
        if database.slave:
            configs['relaylog'] = True
        if bond:
            _host, _port = self._get_entity(req=req, entity=int(bond.reflection_id))
            repl = privilegeutils.mysql_replprivileges(bond, _host)
            configs['binlog'] = True
            _slave = {
                'repl': repl,
                'configs': configs,
            }
            body.update(_slave)
        entity = entity_controller.create(req=req,
                                          agent_id=agent_id,
                                          endpoint=common.DB,
                                          body=body)['data'][0]['entity']
        database.impl = 'local'
        database.status = common.UNACTIVE
        database.reflection_id = str(entity)
        host, port = self._get_entity(req=req, entity=int(bond.reflection_id), raise_error=True)

        if bond:
            try:
                self._bond_slave(req,
                                 database, host, port,
                                 bond, _host, _port,
                                 repl)
            except Exception:
                self._stop_database(database)
                with self._delete_database(session, database):
                    LOG.info('Delete database success')
                raise
            else:
                session.add(GopSalveRelation(database.database_id, bond.database_id))
                session.flush()
        yield host, port

    def _esure_create(self, database, **kwargs):
        entity_controller.post_create_entity(entity=int(database.reflection_id),
                                             endpoint=common.DB, database_id=database.database_id,
                                             dbtype=database.dbtype)

    @contextlib.contextmanager
    def _delete_database(self, session, database, **kwargs):
        req = kwargs.pop('req')
        local_ip, port = self._get_entity(req=req, entity=int(database.reflection_id), raise_error=True)
        token = uuidutils.generate_uuid()
        entity_controller.delete(req=req, endpoint=common.DB, entity=int(database.reflection_id),
                                 body=dict(token=token))
        yield local_ip, port

    @contextlib.contextmanager
    def _delete_slave_database(self, session, slave, masters, **kwargs):
        req = kwargs.pop('req')
        local_ip, port = self._get_entity(req, int(slave.reflection_id), raise_error=True)
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
            if local_ip == 'unkonwn' or port == 0:
                raise exceptions.AcceptableDbError('Database not online')
            connection = connformater % dict(user=database.user, passwd=database.passwd,
                                             host=local_ip, port=port, schema=schema)
            engine = create_engine(connection, thread_checkin=False, poolclass=NullPool)
            utils.create_schema(engine, auths=auths,
                                character_set=options.get('character_set'),
                                collation_type=options.get('collation_type'),
                                connection_timeout=3)
            yield local_ip, port
        except Exception:
            if LOG.isEnabledFor(logging.DEBUG):
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

