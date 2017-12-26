import os
import time
import shutil
import re
import contextlib
import eventlet
import psutil

from collections import namedtuple

from simpleutil.utils import uuidutils
from simpleutil.utils import jsonutils
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
from gopdb import utils

from gopdb.api.client import GopDBClient


CONF = cfg.CONF

LOG = logging.getLogger(__name__)


CREATESCHEMA = {
    'type': 'object',
    'required': [common.ENDPOINTKEY, 'uri', 'dbtype'],
    'properties':
        {
            common.ENDPOINTKEY:  {'type': 'string', 'description': 'endpoint name of database resource'},
            'dbtype': {'type': 'string', 'description': 'database type mysql only now'},
            'uri': {'type': 'string', 'description': 'impl checkout uri'},
            'version': {'type': 'string'},
            'auth': {'type': 'object'},
            'esure': {'type': 'boolean'},
            'timeout': {'type': 'integer', 'minimum': 3, 'maxmum': 3600},
            'cdnhost': {'type': 'object',
                        'required': ['hostname'],
                        'properties': {'hostname': {'type': 'string'},
                                       'listen': {'type': 'integer', 'minimum': 1, 'maxmum': 65535},
                                       'charset': {'type': 'string'},
                                       }},
        }
}


def count_timeout(ctxt, kwargs):
    deadline = ctxt.get('deadline')
    timeout = kwargs.pop('timeout', None)
    if deadline is None:
        return timeout
    deadline = deadline - int(time.time())
    if timeout is None:
        return deadline
    return min(deadline, timeout)


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

    def post_start(self):
        super(Application, self).post_start()
        pids = utils.find_process()
        # reflect entity database_id
        dbmaps = self.client.reflect_database(impl='local', body=dict(entitys=self.entitys))['data']
        for dbinfo in dbmaps:
            _entity = int(dbinfo.get('entity'))
            _database_id = dbinfo.get('database_id')
            if _entity in self.konwn_database:
                raise RuntimeError('Database Entity %d Duplicate' % _entity)
            LOG.info('entity %d with database id %d' % (_entity, _database_id))
            self.konwn_database.setdefault(_entity, dict(database_id=_database_id, pid=None))
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
        dbtype = kwargs.pop('dbtype')
        configs = kwargs.pop('configs', {})

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
                configs.setdefault('entity', entity)
                configs.setdefault('port', ports[0])
                configs.setdefault('datadir', self.apppath(entity))
                configs.setdefault('pidfile', pidfile)
                configs.setdefault('sockfile', sockfile)
                configs.setdefault('logfile', logfile)
                configs.setdefault('runuser', self.entity_user(entity))
                dbmanager.save_conf(cfgfile, **configs)
                LOG.info('Prepare database config file success')

                def _notify_success():
                    """notify database intance create success"""
                    dbinfo = self.konwn_database.get(entity)
                    if not dbinfo:
                        LOG.warning('Can not find entity database id, active fail')
                        return
                    if self._entity_process(entity):
                        self.client.database_update(database_id=dbinfo.get('database_id'),
                                                    body={'status': common.OK})

                kwargs.update({'logfile': install_log})
                # call database_install in green thread
                # eventlet.spawn_n(dbmanager.install, cfgfile, _notify_success, timeout,
                #                  **kwargs)
                threadpool.add_thread(dbmanager.install, cfgfile, _notify_success, timeout,
                                      **kwargs)

        def _port_notity():
            """notify port bond"""
            self.client.ports_add(agent_id=self.manager.agent_id,
                                  endpoint=common.DB, entity=entity, ports=ports)

        threadpool.add_thread(_port_notity)
        return port

    def rpc_create_entity(self, ctxt, entity, **kwargs):
        # jsonutils.schema_validate(kwargs, CREATESCHEMA)
        entity = int(entity)
        with self.lock(entity, timeout=3):
            if entity in self.entitys:
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt,
                                                  result='create %s database fail, entity exist' % entity)
            timeout = count_timeout(ctxt, kwargs)
            try:
                self.create_entity(entity, timeout, **kwargs)
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
                                          result=result,
                                          details=[dict(detail_id=entity,
                                                   resultcode=resultcode,
                                                   result='wait database start')])

    def rpc_post_create_entity(self, ctxt, entity, **kwargs):
        database_id = kwargs.pop('database_id')
        self.konwn_database.setdefault(entity, dict(database_id=database_id, pid=None))

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
        dbtype = kwargs.pop('dbtype')
        dbmanager = utils.impl_cls('rpc', dbtype)
        cfgfile = self._db_conf(entity, dbtype)
        with self.lock(entity, timeout=3):
            if self._entity_process(entity):
                raise
            dbmanager.start(cfgfile)
            eventlet.sleep(0.5)
        p = self._entity_process(entity)
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          result='start entity success',
                                          details=[dict(detail_id=entity,
                                                   resultcode=manager_common.RESULT_SUCCESS,
                                                   result=p.info)])

    def rpc_stop_entity(self, ctxt, entity, **kwargs):
        dbtype = kwargs.pop('dbtype')
        dbmanager = utils.impl_cls('rpc', dbtype)
        p = self._entity_process(entity)
        p.terminal()


    def rpc_status_entity(self, ctxt, entity, **kwargs):
        p = self._entity_process(entity)