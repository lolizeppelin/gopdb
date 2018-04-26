# -*- coding: utf-8 -*-
import os
import time
import shutil
import re
import contextlib
import eventlet
import psutil

from simpleutil.utils import singleton
from simpleutil.utils import systemutils
from simpleutil.log import log as logging
from simpleutil.config import cfg

from simpleservice.loopingcall import IntervalLoopinTask

from goperation import threadpool
from goperation.utils import safe_func_wrapper
from goperation.manager.api import get_http
from goperation.manager import common as manager_common
from goperation.manager.rpc.agent.application.base import AppEndpointBase

from goperation.manager.utils import resultutils
from goperation.manager.utils import validateutils
from goperation.manager.rpc.exceptions import RpcTargetLockException


from gopdb import common
from gopdb import privilegeutils
from gopdb import utils
from gopdb.api.rpc import impl as IMPL

from gopdb.api.client import GopDBClient


CONF = cfg.CONF

LOG = logging.getLogger(__name__)


def count_timeout(ctxt, kwargs):
    deadline = ctxt.get('deadline')
    timeout = kwargs.pop('timeout', None)
    if deadline is None:
        return timeout
    deadline = deadline - int(time.time())
    if timeout is None:
        return deadline
    return min(deadline, timeout)


class CreateResult(resultutils.AgentRpcResult):
    def __init__(self, agent_id, ctxt,
                 resultcode, result,
                 connection, port):
        super(CreateResult, self).__init__(agent_id, ctxt, resultcode, result)
        self.connection = connection
        self.port = port

    def to_dict(self):
        ret_dict = super(CreateResult, self).to_dict()
        ret_dict.setdefault('port', self.port)
        ret_dict.setdefault('connection', self.connection)
        return ret_dict


@singleton.singleton
class Application(AppEndpointBase):

    def __init__(self, manager):
        group = CONF.find_group(common.DB)
        super(Application, self).__init__(manager, group.name)
        self.client = GopDBClient(get_http())
        self.delete_tokens = {}
        self.konwn_database = {}

    @property
    def apppathname(self):
        return 'database'

    @property
    def logpathname(self):
        return 'dblog'

    def entity_user(self, entity):
        return 'gopdb-%d' % entity

    def entity_group(self, entity):
        return 'gopdb'

    def pre_start(self, external_objects):
        super(AppEndpointBase, self).pre_start(external_objects)
        external_objects.update(common.VERSIONMAP)
        external_objects.update({'gopdb-aff': CONF[common.DB].affinity})

    def post_start(self):
        super(Application, self).post_start()
        pids = utils.find_process()
        # reflect entity database_id
        dbmaps = self.client.reflect_database(impl='local', body=dict(entitys=self.entitys))['data']
        for dbinfo in dbmaps:
            _entity = int(dbinfo.pop('entity'))
            _database_id = dbinfo.pop('database_id')
            _dbtype = dbinfo.pop('dbtype')
            slave = dbinfo.pop('slave')
            if _entity in self.konwn_database:
                raise RuntimeError('Database Entity %d Duplicate' % _entity)
            LOG.info('entity %d with database id %d' % (_entity, _database_id))
            self.konwn_database.setdefault(_entity, dict(database_id=_database_id,
                                                         dbtype=_dbtype,
                                                         slave=slave,
                                                         pid=None))
        # find entity pid
        for entity in self.entitys:
            _pid = self._find_from_pids(entity, pids)
            if _pid:
                LOG.info('Database entity %d is running at %d' % (entity, _pid))
                self.konwn_database[entity]['pid'] = _pid

    def _esure(self, entity, username, cmdline):
        datadir = False
        runuser = False
        if username == self.entity_user(entity):
            runuser = True
        pattern = re.compile('%s' % self.apppath(entity))
        for cmd in cmdline:
            if re.search(pattern, cmd):
                datadir = True
                break
        if datadir and runuser:
            return True
        if datadir and not runuser:
            LOG.error('entity %d with %s run user error' % (entity, self.apppath(entity)))
            raise ValueError('Runuser not %s' % self.entity_user(entity))
        return False

    def _find_from_pids(self, entity, pids=None, impl=None):
        if not pids:
            pids = utils.find_process(impl)
        for info in pids:
            if self._esure(entity, info.get('username'), info.get('cmdline')):
                return info.get('pid')

    def _db_conf(self, entity, dbtype):
        return os.path.join(self.entity_home(entity), '%s.conf' % dbtype)

    def _dbtype(self, entity):
        return self.konwn_database[entity].get('dbtype')

    @contextlib.contextmanager
    def _allocate_port(self, entity, port):
        with self.manager.frozen_ports(common.DB, entity, ports=[port, ]) as ports:
            yield list(ports)

    def _free_port(self, entity):
        ports = self.manager.allocked_ports.get(common.DB)[entity]
        self.manager.free_ports(ports)

    def _entity_process(self, entity):
        entityinfo = self.konwn_database.get(entity)
        if not entityinfo:
            raise ValueError('Entity not found')
        _pid = entityinfo.get('pid')
        if _pid:
            try:
                p = psutil.Process(pid=_pid)
                if self._esure(entity, p.username(), p.cmdline()):
                    info = dict(pid=p.pid, exe=p.exe(), cmdline=p.cmdline(), username=p.username())
                    setattr(p, 'info', info)
                    return p
            except psutil.NoSuchProcess:
                _pid = None
        if not _pid:
            _pid = self._find_from_pids(entity)
        if not _pid:
            self.konwn_database[entity]['pid'] = None
            return None
        try:
            p = psutil.Process(pid=_pid)
            info = dict(pid=p.pid, exe=p.exe(), cmdline=p.cmdline(), username=p.username())
            setattr(p, 'info', info)
            self.konwn_database[entity]['pid'] = _pid
            return p
        except psutil.NoSuchProcess:
            self.konwn_database[entity]['pid'] = None
            return None

    def delete_entity(self, entity, token):
        if token != self._entity_token(entity):
            raise ValueError('Delete token error')
        if self._entity_process(entity):
            raise ValueError('Target entity is running')
        LOG.info('Try delete %s entity %d' % (self.namespace, entity))
        home = self.entity_home(entity)
        if os.path.exists(home):
            try:
                shutil.rmtree(home)
            except Exception:
                LOG.exception('delete error')
        self._free_port(entity)
        self.entitys_map.pop(entity, None)
        self.konwn_database.pop(entity, None)
        systemutils.drop_user(self.entity_user(entity))

    def create_entity(self, entity, timeout, **kwargs):
        """
        @param dbtype:        string 数据库类型
        @param configs:       dict   数据库配置字典
        @param auth:          dict   远程管理员账号密码
        @param bond:          dict   需要绑定的从库(主库专用参数)
        """
        dbtype = kwargs.pop('dbtype')
        configs = kwargs.pop('configs', {})
        bond = kwargs.pop('bond', None)
        if bond:
            replication = privilegeutils.mysql_replprivileges(bond.get('database_id'), bond.get('host'))
            kwargs['replication'] = replication
        port = configs.pop('port', None)
        pidfile = os.path.join(self.entity_home(entity), '%s.pid' % dbtype)
        sockfile = os.path.join(self.entity_home(entity), '%s.sock' % dbtype)
        logfile = os.path.join(self.logpath(entity), '%s.log' % dbtype)
        install_log = os.path.join(self.logpath(entity), 'install.log')
        cfgfile = self._db_conf(entity, dbtype)
        LOG.info('Load database manager for %s' % dbtype)
        dbmanager = utils.impl_cls('rpc', dbtype)

        with self._prepare_entity_path(entity, apppath=False):
            with self._allocate_port(entity, port) as ports:
                port = ports[0]
                configs.setdefault('entity', entity)
                configs.setdefault('port', port)
                configs.setdefault('datadir', self.apppath(entity))
                configs.setdefault('pidfile', pidfile)
                configs.setdefault('sockfile', sockfile)
                configs.setdefault('logfile', logfile)
                configs.setdefault('runuser', self.entity_user(entity))
                dbmanager.save_conf(cfgfile, **configs)
                LOG.info('Prepare database config file success')

                def _notify_success(results):
                    """notify database intance create success"""
                    dbinfo = self.konwn_database.get(entity)
                    if not dbinfo:
                        LOG.warning('Can not find entity database id, active fail')
                        return
                    if bond:
                        LOG.debug('Try bond slave database')
                        binlog = results[-1][0]
                        self.client.database_bond(database_id=bond.get('database_id'),
                                                  body={'master': dbinfo.get('database_id'),
                                                        'host': self.manager.local_ip,
                                                        'port': port,
                                                        'passwd': replication.get('passwd'),
                                                        'file': binlog[0],
                                                        'position': binlog[1],
                                                        })
                    if self._entity_process(entity):
                        self.client.database_update(database_id=dbinfo.get('database_id'),
                                                    body={'status': common.OK})
                kwargs.update({'logfile': install_log})
                threadpool.add_thread(dbmanager.install, cfgfile, _notify_success, timeout,
                                      **kwargs)

        # def _port_notify():
        #     """notify port bond"""
        #     _timeout = timeout if timeout else 30
        #     overtime = int(time.time()) + _timeout
        #     while entity not in self.konwn_database:
        #         if int(time.time()) > overtime:
        #             LOG.error('Fail allocate port %d for %s.%d' % (ports[0], common.DB, entity))
        #             return
        #         eventlet.sleep(1)
        #     self.client.ports_add(agent_id=self.manager.agent_id,
        #                           endpoint=common.DB, entity=entity, ports=ports)

        # threadpool.add_thread(_port_notify)
        return port

    def rpc_create_entity(self, ctxt, entity, **kwargs):
        memory = psutil.virtual_memory()
        leftmem = memory.cached / (1024 * 1024) + memory.free / (1024 * 1024)
        if leftmem < 1000:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='create database fail, memory left %d MB' % leftmem)
        entity = int(entity)
        with self.lock(entity, timeout=3):
            if entity in self.entitys:
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt,
                                                  result='create %s database fail, entity exist' % entity)
            timeout = count_timeout(ctxt, kwargs)
            try:
                port = self.create_entity(entity, timeout, **kwargs)
                resultcode = manager_common.RESULT_SUCCESS
                result = 'create database success'
            except Exception as e:
                resultcode = manager_common.RESULT_ERROR
                result = 'create database fail with %s:%s' % (e.__class__.__name__,
                                                              str(e.message)
                                                              if hasattr(e, 'message') else 'unknown err msg')
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  ctxt=ctxt,
                                                  resultcode=resultcode,
                                                  result=result, )
        return CreateResult(agent_id=self.manager.agent_id,
                            ctxt=ctxt,
                            resultcode=resultcode,
                            result=result,
                            connection=self.manager.local_ip,
                            port=port)

    def rpc_post_create_entity(self, ctxt, entity, **kwargs):
        database_id = kwargs.pop('database_id')
        dbtype = kwargs.pop('dbtype')
        slave = kwargs.pop('slave')
        self.konwn_database.setdefault(entity, dict(database_id=database_id,
                                                    slave=slave,
                                                    dbtype=dbtype, pid=None))

    def rpc_reset_entity(self, ctxt, entity, **kwargs):
        entity = int(entity)
        pass

    def rpc_delete_entity(self, ctxt, entity, **kwargs):
        entity = int(entity)
        token = kwargs.pop('token')
        timeout = count_timeout(ctxt, kwargs if kwargs else {})
        while self.frozen:
            if timeout < 1:
                raise RpcTargetLockException(self.namespace, str(entity), 'endpoint locked')
            eventlet.sleep(1)
            timeout -= 1
        timeout = min(1, timeout)
        details = []
        with self.lock(entity, timeout):
            if entity not in set(self.entitys):
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt, result='delete database fail, entity not exist')
            try:
                self.delete_entity(entity, token)
                resultcode = manager_common.RESULT_SUCCESS
                result = 'delete %d success' % entity
            except Exception as e:
                resultcode = manager_common.RESULT_ERROR
                result = 'delete %d fail with %s:%s' % (entity, e.__class__.__name__,
                                                        str(e.message) if hasattr(e, 'message') else 'unknown err msg')
        details.append(dict(detail_id=entity,
                            resultcode=resultcode,
                            result=result))
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          resultcode=resultcode,
                                          result=result,
                                          details=details)

    def rpc_start_entity(self, ctxt, entity, **kwargs):
        dbtype = self._dbtype(entity)
        dbmanager = utils.impl_cls('rpc', dbtype)
        cfgfile = self._db_conf(entity, dbtype)
        p = self._entity_process(entity)
        with self.lock(entity, timeout=3):
            if not p:
                dbmanager.start(cfgfile)
                eventlet.sleep(0.5)
                p = self._entity_process(entity)
        if not p:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='start entity faile, process not exist after start')
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          result='start entity success, runinng on pid %d' % p.info.get('pid'))

    def rpc_stop_entity(self, ctxt, entity, **kwargs):
        dbtype = self._dbtype(entity)
        dbmanager = utils.impl_cls('rpc', dbtype)
        p = self._entity_process(entity)
        if p:
            cfgfile = self._db_conf(entity, dbtype)
            dbmanager.stop(cfgfile, postrun=None, timeout=None, process=p)
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          result='stop database entity success')

    def rpc_status_entity(self, ctxt, entity, **kwargs):
        dbtype = self._dbtype(entity)
        p = self._entity_process(entity)
        database_id = self.konwn_database[entity].get('database_id')
        if not p:
            result = '%s entity %d(database_id %d) not running' % (dbtype, entity, database_id)
        else:
            result = '%s entity %d(database_id %d) running at pid %d' % (dbtype, entity, database_id, p.pid)

        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          result=result)

    def rpc_bond_entity(self, ctxt, entity, **kwargs):
        dbtype = self._dbtype(entity)
        dbmanager = utils.impl_cls('rpc', dbtype)
        cfgfile = self._db_conf(entity, dbtype)
        with self.lock(entity, timeout=3):
            p = self._entity_process(entity)
            if not p:
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  ctxt=ctxt,
                                                  result='bond entity faile, process not exist')
            dbmanager.bond(cfgfile, postrun=None, timeout=None, **kwargs)
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          resultcode=manager_common.RESULT_ERROR,
                                          ctxt=ctxt,
                                          result='start bond fail, ')
