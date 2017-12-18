import os
import time
import shutil
import contextlib
import inspect
import eventlet

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
            common.ENDPOINTKEY:  {'type': 'string', 'description': 'endpoint name of cdn resource'},
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
    timeout = kwargs.get('timeout')
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
        self.konwn_pids = {}

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

    def _db_conf(self, entity, dbtype):
        return os.path.join(self.entity_home(entity), '%s.conf' % dbtype)

    @contextlib.contextmanager
    def _allocate_port(self, entity, port):
        with self.manager.frozen_ports(common.DB, entity, ports=[port, ]) as ports:
            yield list(ports)

    def _free_port(self, entity):
        ports = self.manager.allocked_ports.get(common.DB)[entity]
        self.manager.free_ports(self, ports)

    def _entity_status(self, entity, detail=False):
        pass

    def delete_entity(self, entity, token):
        if token != self._entity_token(entity):
            raise
        if self._entity_status(entity):
            raise
        LOG.info('Try delete %s entity %d' % (self.namespace, entity))
        home = self.entity_home(entity)
        if os.path.exists(home):
            try:
                shutil.rmtree(home)
            except Exception:
                LOG.exception('delete error')
                raise
            else:
                self._free_port(entity)
                self.entitys_map.pop(entity)

    def create_entity(self, entity, **kwargs):
        dbtype = kwargs.pop('dbtype')
        # auth = kwargs.pop('auth')
        timeout = kwargs.pop('timeout')
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
                configs.setdefault('port', ports[0])
                configs.setdefault('datadir', self.apppath(entity))
                configs.setdefault('pidfile', pidfile)
                configs.setdefault('sockfile', sockfile)
                configs.setdefault('logfile', logfile)
                configs.setdefault('runuser', self.entity_user(entity))


                # prepare database config file
                dbmanager.save_conf(cfgfile, **configs)

                def _notify_success():
                    self.client.ports_add(agent_id=self.manager.agent_id,
                                          endpoint=common.DB, entity=entity, ports=ports)
                    eventlet.sleep(1)
                    try:
                        database_id = self.konwn_database.pop(entity)
                    except KeyError:
                        LOG.warning('Can not find entity database id, active fail')
                        return
                    self.client.database_update(database_id=database_id, body={'status': common.OK})

                # call database_install in green thread
                eventlet.spawn_n(dbmanager.install, cfgfile, install_log,
                                 postrun=_notify_success, timeout=timeout, **kwargs)
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
            kwargs.update({'timeout':  timeout})
            try:
                port = self.create_entity(entity, **kwargs)
                resultcode = manager_common.RESULT_SUCCESS
                result = 'create database success'
            except Exception as e:
                resultcode = manager_common.RESULT_ERROR
                result = 'create database fail with %s' % e.__class__.__name__

        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          resultcode=resultcode,
                                          result=result,
                                          details=[dict(detail_id=entity,
                                                   resultcode=resultcode,
                                                   result='result')])

    def rpc_post_create_entity(self, ctxt, entity, **kwargs):
        database_id = kwargs.pop('database_id')
        self.konwn_database.setdefault(entity, database_id)

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
                result = 'delete %d fail with %s' % (entity, e.__class__.__name__)
        details.append(dict(detail_id=entity,
                            resultcode=resultcode,
                            result=result))
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          resultcode=resultcode,
                                          result=result,
                                          details=details)

    def rpc_start_entity(self, ctxt, entity, **kwargs):
        pass

    def rpc_stop_entity(self, ctxt, entity, **kwargs):
        pass

    def rpc_status_entity(self, ctxt, entity, **kwargs):
        pass