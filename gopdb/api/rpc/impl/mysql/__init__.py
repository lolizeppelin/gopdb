import six
import os
import eventlet
import ConfigParser
from collections import OrderedDict
import psutil
import mysql.connector

from simpleutil.log import log as logging
from simpleutil.config import cfg
from simpleutil.utils import systemutils

from simpleservice.ormdb import engines
from sqlalchemy.pool import NullPool

import goperation

from gopdb import common
from gopdb import privilegeutils
from gopdb.api.rpc.impl import DatabaseConfigBase
from gopdb.api.rpc.impl import DatabaseManagerBase

from gopdb.api.rpc.impl.mysql import config

from goperation.utils import safe_fork

if systemutils.POSIX:
    from simpleutil.utils.systemutils.posix import wait
    MYSQLSAFE = systemutils.find_executable('mysqld_safe')
    MYSQLINSTALL = systemutils.find_executable('mysql_install_db')
    SH = systemutils.find_executable('sh')
    BASELOG = '/var/log/mysqld.log'
else:
    # just for windows test
    from simpleutil.utils.systemutils import empty as wait
    MYSQLSAFE = r'C:\Program Files\mysql\bin\mysqld_safe.exe'
    MYSQLINSTALL = r'C:\Program Files\mysql\bin\mysql_install_db.exe'
    SH = r'C:\Program Files\Git\bin\sh.exe'
    BASELOG = r'C:\temp\mysqld.log'


CONF = cfg.CONF

LOG = logging.getLogger(__name__)


config.register_opts(CONF.find_group(common.DB))

MULTIABLEOPTS = frozenset([
    'replicate-ignore-db',
])


# def test():
#     try:
#         if master_host == slave_host:
#             raise exceptions.UnAcceptableDbError('Master and Salve in same host')
#         # master do
#         connection = connformater % dict(user=master.user, passwd=master.passwd,
#                                          host=master_host, port=master_port, schema='')
#         engine = create_engine(connection, thread_checkin=False, poolclass=NullPool)
#         with engine.connect() as conn:
#             LOG.info('Login master database to get pos and file')
#             r = conn.execute('show master status')
#             results = r.fetchall()
#             r.close()
#         if not results:
#             raise exceptions.UnAcceptableDbError('Master bind log not open!')
#         binlog = results[0]
#         if binlog.get('file')[-1] != '1' or binlog.get('position') > 1000:
#             raise exceptions.UnAcceptableDbError('Database pos of file error')
#         # slave do
#         slave_info = dict(replname='database-%d' % master.database_id,
#                           host=master_host, port=master_port,
#                           repluser=repl.get('user'), replpasswd=repl.get('passwd'),
#                           file=binlog.get('file'), pos=binlog.get('position'))
#         sqls = ['SHOW SLAVE STATUS']
#         sqls.append("CHANGE MASTER '%(replname)s' TO MASTER_HOST='%(host)s', MASTER_PORT=%(port)d," \
#                     "MASTER_USER='%(repluser)s',MASTER_PASSWORD='%(replpasswd)s'," \
#                     "MASTER_LOG_FILE='%(file)s',MASTER_LOG_POS=%(pos)s)" % slave_info)
#         sqls.append('START salve %(replname)s' % slave_info)
#
#         connection = connformater % dict(user=slave.user, passwd=slave.passwd,
#                                          host=slave_host, port=slave_port, schema='')
#         engine = create_engine(connection, thread_checkin=False, poolclass=NullPool)
#         with engine.connect() as conn:
#             LOG.info('Login slave database for init')
#             r = conn.execute(sqls[0])
#             if LOG.isEnabledFor(logging.DEBUG):
#                 for row in r.fetchall():
#                     LOG.debug(str(row))
#             r.close()
#             r = conn.execute(sqls[1])
#             r.close()
#             LOG.debug('Success add repl info')
#             try:
#                 r = conn.execute(sqls[2])
#             except Exception:
#                 LOG.error('Start slave fail')
#                 raise exceptions.UnAcceptableDbError('Start slave fail')
#             else:
#                 r.close()
#     except exceptions.UnAcceptableDbError:
#         raise
#     except Exception as e:
#         if LOG.isEnabledFor(logging.DEBUG):
#             LOG.exception('Bond slave fail')
#         raise exceptions.UnAcceptableDbError('Bond slave fail with %s' % e.__class__.__name__)


class MultiOrderedDict(OrderedDict):
    def __setitem__(self, key, value,
                    dict_setitem=dict.__setitem__):
        if key in MULTIABLEOPTS and key in self:
            if isinstance(self[key], list):
                self[key].append(value)
                return
            else:
                value = [self[key], value]
        OrderedDict.__setitem__(self, key, value, dict_setitem)


class MultiConfigParser(ConfigParser.ConfigParser):
    def __init__(self):
        ConfigParser.ConfigParser.__init__(self, dict_type=MultiOrderedDict)

    def write(self, fp):
        """Write an .ini-format representation of the configuration state."""
        if self._defaults:
            fp.write("[%s]\n" % ConfigParser.DEFAULTSECT)
            for (key, value) in self._defaults.items():
                fp.write("%s = %s\n" % (key, str(value).replace('\n', '\n\t')))
            fp.write("\n")
        for section in self._sections:
            fp.write("[%s]\n" % section)
            for (key, value) in self._sections[section].items():
                if key == "__name__":
                    continue
                if (value is not None) or (self._optcre == self.OPTCRE):
                    if isinstance(value, list):
                        for v in value:
                            line = " = ".join((key, str(v).replace('\n', '\n\t')))
                            fp.write("%s\n" % (line))
                    else:
                        key = " = ".join((key, str(value).replace('\n', '\n\t')))
                        fp.write("%s\n" % (key))
            fp.write("\n")


def default_config():
    cf = MultiConfigParser()

    cf.add_section('mysqld_safe')
    cf.add_section('mysqld')

    # mysqld_safe opts
    cf.set('mysqld_safe', 'log-error', '/var/log/mysqld.log')
    # base options

    cf.set('mysqld', 'server-id', 1)
    cf.set('mysqld', 'symbolic-links', 0)
    cf.set('mysqld', 'character-set-server', 'utf8')
    cf.set('mysqld', 'user', 'mysql')
    cf.set('mysqld', 'pid-file', '/var/run/mysqld/mysqld.pid')
    cf.set('mysqld', 'log-error', '/var/log/mysqld.log')
    cf.set('mysqld', 'datadir', '/data/mysql/mysqld')

    # network config
    cf.set('mysqld', 'socket', '/var/lib/mysql/mysql.sock')
    cf.set('mysqld', 'port', 3306)
    cf.set('mysqld', 'max_connect_errors', 64)
    cf.set('mysqld', 'back_log', 1024)
    cf.set('mysqld', 'max_connections', 512)
    cf.set('mysqld', 'thread_cache_size', 16)
    cf.set('mysqld', 'max_allowed_packet', 33554432)

    # table options
    cf.set('mysqld', 'max_heap_table_size', 268435456)
    cf.set('mysqld', 'tmp_table_size', 268435456)
    cf.set('mysqld', 'table_open_cache', 2048)

    # query options
    cf.set('mysqld', 'query_cache_size', 134217728)

    # InnoDB opts
    cf.set('mysqld', 'innodb_file_per_table', 1)
    cf.set('mysqld', 'innodb_buffer_pool_size', 4294967296)
    cf.set('mysqld', 'innodb_data_file_path', 'ibdata:100M:autoextend')
    cf.set('mysqld', 'innodb_log_buffer_size', 8388608)
    cf.set('mysqld', 'innodb_log_file_size', 536870912)
    cf.set('mysqld', 'innodb_log_files_in_group', 2)
    cf.set('mysqld', 'innodb_flush_log_at_trx_commit', 2)
    cf.set('mysqld', 'innodb_open_files', 1024)

    #  MyISAM opts
    cf.set('mysqld', 'concurrent_insert', 2)
    cf.set('mysqld', 'key_buffer_size', 134217728)
    cf.set('mysqld', 'read_buffer_size', 4194304)
    cf.set('mysqld', 'read_rnd_buffer_size', 6291456)
    return cf


def slave_config(cf):
    # Slave opts
    cf.set('mysqld', 'read-only', 1)
    cf.set('mysqld', 'relay-log', 'relay-bin')
    cf.set('mysqld', 'replicate-ignore-db', 'information_schema')
    cf.set('mysqld', 'replicate-ignore-db', 'performance_schema')
    cf.set('mysqld', 'replicate-ignore-db', 'sys')


class MysqlConfig(DatabaseConfigBase):

    def __init__(self, config):
        # default opts
        if not isinstance(config, MultiConfigParser):
            raise TypeError('mysql config not ConfigParser')
        self.config = config

    def get(self, key):
        return self.config.get('mysqld', key)

    @classmethod
    def load(cls, cfgfile):
        """load config from config file"""
        config = MultiConfigParser()
        config.read(cfgfile)
        return cls(config)

    @classmethod
    def loads(cls, **kwargs):
        """load config from kwargs"""
        mysql_id = kwargs.pop('entity')
        datadir = kwargs.pop('datadir')
        runuser = kwargs.pop('runuser')
        pidfile = kwargs.pop('pidfile')
        sockfile = kwargs.pop('sockfile')
        logfile = kwargs.pop('logfile')
        # binlog on/off
        binlog = (kwargs.pop('binlog', False) or kwargs.pop('log-bin', False))
        relaylog = (kwargs.pop('relaylog', False) or kwargs.pop('relay-bin', False))
        # init mysql default config
        config = default_config()
        # set mysqld_safe opts
        config.set('mysqld_safe', 'log-error', logfile)
        # read opts from kwargs
        for k, v in six.iteritems(kwargs):
            config.set('mysqld', k, v)
        # set default opts
        config.set('mysqld', 'server-id', mysql_id)
        config.set('mysqld', 'datadir', datadir)
        config.set('mysqld', 'pid-file', pidfile)
        config.set('mysqld', 'log-error', logfile)
        config.set('mysqld', 'user', runuser)
        # set default socket opts
        config.set('mysqld', 'socket', sockfile)
        # ste logbin
        if binlog:
            conf = CONF[common.DB]
            config.set('mysqld', 'log-bin', 'binlog')
            config.set('mysqld', 'expire_logs_days', conf.expire_log_days)
            config.set('mysqld', 'max_binlog_size', 270532608)
        if relaylog:
            slave_config(config)
        return cls(config)

    def save(self, cfgfile):
        """save config"""
        with open(cfgfile, 'wb') as f:
            self.config.write(f)

    def dump(self, cfgfile):
        """out put config"""
        output = {}
        for section in self.config.sections():
            output[section] = self.config.items(section)
        return output

    def update(self, cfgfile):
        """update config"""
        config = MultiConfigParser()
        config.read(cfgfile)
        self.config = config

    def binlog(self):
        config = self.config
        if not self.get('log-bin'):
            conf = CONF[common.DB]
            config.set('mysqld', 'log-bin', 'binlog')
            config.set('mysqld', 'expire_logs_days', conf.expire_log_days)
            config.set('mysqld', 'max_binlog_size', 270532608)

class DatabaseManager(DatabaseManagerBase):

    config_cls = MysqlConfig

    base_opts = ['--skip-name-resolve']

    def _slave_status(self, user, passwd, sockfile):
        try:
            conn = mysql.connector.connect(unix_socket=sockfile,
                                           user=user,
                                           password=passwd)
            cursor = conn.cursor(dictionary=True)
            cursor.execute('SHOW ALL SLAVES STATUS')
            cursor.clsoe()
            conn.close()
            slaves = cursor.fetchall()
        except Exception:
            LOG.error('Get slave status fail')
            raise
        return slaves

    def _master_status(self, user, passwd, sockfile):
        try:
            conn = mysql.connector.connect(unix_socket=sockfile,
                                           user=user,
                                           password=passwd)
            cursor = conn.cursor(dictionary=True)
            cursor.execute('SHOW MASTER STATUS')
            cursor.clsoe()
            conn.close()
            masters = cursor.fetchall()
        except Exception:
            LOG.error('Get master status fail')
            raise
        return masters

    def status(self, cfgfile, **kwargs):
        """status of database intance"""

    def start(self, cfgfile, postrun=None, timeout=None, **kwargs):
        """stary database intance"""
        args = [SH, MYSQLSAFE, '--defaults-file=%s' % cfgfile]
        args.extend(self.base_opts)
        if not systemutils.POSIX:
            # just for test on windows
            LOG.info('will call %s', ' '.join(args))
            return
        pid = safe_fork()
        if pid == 0:
            # fork twice
            ppid = os.fork()
            if ppid == 0:
                os.closerange(3, systemutils.MAXFD)
                with open(BASELOG, 'ab') as f:
                    os.dup2(f.fileno(), 1)
                    os.dup2(f.fileno(), 2)
                os.execv(SH, args)
            else:
                os._exit(0)
        else:
            wait(pid)

    def stop(self, cfgfile, postrun, timeout,
             **kwargs):
        """stop database intance"""
        process = kwargs.pop('process', None)
        config = self.config_cls.load(cfgfile)
        pidifle = config.get('pid-file')
        datadir = config.get('datadir')
        user = config.get('user')
        with open(pidifle, 'rb') as f:
            _pid = int(f.read(4096).strip())
            if process:
                if _pid != process.pid:
                    raise ValueError('Process pid not match pid file')
            else:
                process = psutil.Process(_pid)
        cmdlines = process.cmdline()
        if process.username() == user and '--datadir=%s' % datadir in cmdlines:
            process.terminate()
        else:
            raise ValueError('Process user or cmdline not match')

    def bond(self, cfgfile, postrun, timeout, dbinfo,
             **kwargs):
        """bond to master database intance"""
        conf = CONF[common.DB]
        master = kwargs.pop('master')
        force = kwargs.pop('force', False)
        config = self.config_cls.load(cfgfile)
        sockfile = config.get('socket')
        LOG.info('Try bond master for mysql %s' % sockfile)
        conn = 'mysql+mysqlconnector://%s:%s@localhost/mysql?unix_socket=%s' % (conf.localroot,
                                                                                conf.localpass,
                                                                                sockfile)
        engine = engines.create_engine(sql_connection=conn,
                                       poolclass=NullPool)
        auth = privilegeutils.mysql_slave_replprivileges(slave_id=dbinfo.get('database_id'), **master)
        master_name = 'masterdb-%(database_id)s' % auth
        sql = "CHANGE MASTER 'masterdb-%(database_id)s' TO MASTER_HOST='%(host)s', MASTER_PORT=%(port)d," \
              "MASTER_USER='%(user)s',MASTER_PASSWORD='%(passwd)s'," \
              "MASTER_LOG_FILE='%(file)s',MASTER_LOG_POS=%(position)s" % auth
        LOG.info('Replication connect sql %s' % sql)
        results = []

        slaves = self._slave_status(user=conf.localroot,
                                    passwd=conf.localpass,
                                    sockfile=sockfile)
        results.append(slaves)

        for slave_status in slaves:
            if slave_status.get('Connection_name') == master_name:
                if LOG.isEnabledFor(logging.DEBUG):
                    for key in slave_status.keys():
                        LOG.debug('BOND FIND OLD SLAVE %s STATUS ------ %s : %s'
                                  % (master_name, key, slave_status[key]))

        with engine.connect() as conn:
            LOG.info('Login mysql from unix sock success, try bond master')

            r = conn.execute(sql)
            if r.returns_rows:
                results.append(r.fetchall())
            r.close()
            LOG.info('Connect success, try start slave')
            r = conn.execute("START SLAVE '%s'" % master_name)
            if r.returns_rows:
                results.append(r.fetchall())
            r.close()
        if postrun:
            postrun(results)

    def unbond(self, cfgfile, postrun, timeout,
               **kwargs):
        """bond to master database intance"""
        conf = CONF[common.DB]
        master = kwargs.pop('master')
        force = kwargs.pop('force', False)
        config = self.config_cls.load(cfgfile)
        sockfile = config.get('socket')
        LOG.info('Try bond master for mysql %s' % sockfile)
        conn = 'mysql+mysqlconnector://%s:%s@localhost/mysql?unix_socket=%s' % (conf.localroot,
                                                                                conf.localpass,
                                                                                sockfile)
        engine = engines.create_engine(sql_connection=conn,
                                       poolclass=NullPool)

        results = []

        slaves = self._slave_status(user=conf.localroot,
                                    passwd=conf.localpass,
                                    sockfile=sockfile)
        results.append(slaves)
        master_name = 'masterdb-%(database_id)s' % master.get('database_id')
        schemas = master.get('schemas')
        ready = master.get('ready')

        for slave_status in slaves:
            if slave_status.get('Connection_name') == master_name:
                if LOG.isEnabledFor(logging.DEBUG):
                    for key in slave_status.keys():
                        LOG.debug('UNBOND SLAVE %s STATUS ------ %s : %s'
                                  % (master_name, key, slave_status[key]))

        with engine.connect() as conn:
            LOG.info('Login mysql from unix sock success, try stop slave then unbond')
            r = conn.execute("STOP SLAVE '%s'" % master_name)
            if r.returns_rows:
                results.append(r.fetchall())
            r.close()

            r = conn.execute("RESET SLAVE '%s'" % master_name)
            if r.returns_rows:
                results.append(r.fetchall())
            r.close()
        if postrun:
            postrun(results)

    def revoke(self, cfgfile, postrun, timeout,
               **kwargs):
        """bond to master database intance"""
        conf = CONF[common.DB]
        auth = kwargs.pop('auth')
        config = self.config_cls.load(cfgfile)
        sockfile = config.get('socket')
        LOG.info('Try bond master for mysql %s' % sockfile)
        conn = 'mysql+mysqlconnector://%s:%s@localhost/mysql?unix_socket=%s' % (conf.localroot,
                                                                                conf.localpass,
                                                                                sockfile)
        engine = engines.create_engine(sql_connection=conn,
                                       poolclass=NullPool)

        sql = "REVOKE %(privileges)s ON %(schema)s.* FROM '%(user)s'@'%(source)s'" % auth
        results = []

        with engine.connect() as conn:
            LOG.info('Login mysql from unix sock success, try revoke')
            r = conn.execute(sql)
            if r.returns_rows:
                results.append(r.fetchall())
            r.close()

        if postrun:
            postrun(results)

    def install(self, cfgfile, postrun, timeout, **kwargs):
        """create database intance"""
        if not os.path.exists(cfgfile):
            raise ValueError('Config file not exist')
        args = [SH, MYSQLINSTALL, '--defaults-file=%s' % cfgfile]
        args.extend(self.base_opts)
        replication = kwargs.pop('replication', None)
        auth = kwargs.pop('auth')
        logfile = kwargs.get('logfile')
        if not systemutils.POSIX:
            # just for test on windows
            LOG.info('will call %s', ' '.join(args))
        else:
            with goperation.tlock('gopdb-install', 30):
                pid = safe_fork()
                if pid == 0:
                    os.closerange(3, systemutils.MAXFD)
                    logfile = logfile or os.devnull
                    with open(logfile, 'wb') as f:
                        os.dup2(f.fileno(), 1)
                        os.dup2(f.fileno(), 2)
                    os.execv(SH, args)
                else:
                    try:
                        wait(pid, timeout)
                    except:
                        raise
                    finally:
                        LOG.info('%s has been exit' % MYSQLINSTALL)
        eventlet.sleep(0)
        self.start(cfgfile)
        eventlet.sleep(3)
        results = self._init_passwd(cfgfile, auth, replication)
        if postrun:
            postrun(results)

    def dump(self, cfgfile, postrun, timeout,
             **kwargs):
        """dump database data"""

    def load_conf(self, cfgfile, **kwargs):
        """out put database config"""

    def save_conf(self, cfgfile, **configs):
        """update database config"""
        dbconfig = self.config_cls.loads(**configs)
        dbconfig.save(cfgfile)
        systemutils.chmod(cfgfile, 022)

    def _init_passwd(self, cfgfile, auth, replication):
        """init password for database"""
        conf = CONF[common.DB]
        config = self.config_cls.load(cfgfile)
        sockfile = config.get('socket')
        LOG.info('Try init password for mysql %s' % sockfile)
        conn = 'mysql+mysqlconnector://%s:%s@localhost/mysql?unix_socket=%s' % ('root', '', sockfile)
        engine = engines.create_engine(sql_connection=conn,
                                       poolclass=NullPool)
        _auth = dict(user=auth.get('user'), passwd=auth.get('passwd'),
                     privileges=common.ALLPRIVILEGES, source=auth.get('source') or '%')
        sqls = ["drop database test",
                "truncate table db",
                "delete from user where host != 'localhost' or user != 'root'",
                "update user set user='%s', password=password('%s') where user='root'" % (conf.localroot,
                                                                                          conf.localpass),
                "grant %(privileges)s on *.* to '%(user)s'@'%(source)s' IDENTIFIED by '%(passwd)s'" % _auth ,
                "grant grant option on *.* to '%(user)s'@'%(source)s'" % dict(user=_auth.get('user'),
                                                                              source=_auth.get('source'))
                ]
        if replication:
            sqls.append("grant %(privileges)s on *.* to '%(user)s'@'%(source)s' IDENTIFIED by '%(passwd)s'"
                        % replication)
        sqls.extend([
            'FLUSH PRIVILEGES',
            'RESET MASTER',
        ])
        results = []
        with engine.connect() as conn:
            LOG.info('Login mysql from unix sock success, try init privileges')
            for sql in sqls:
                LOG.debug(sql)
                r = conn.execute(sql)
                if r.returns_rows:
                    results.append(r.fetchall())
                r.close()
                # if r.returns_rows:
                #     r.fetchall()
        LOG.info('Init privileges finishd')
        results.append(self._master_status(conf.localroot, conf.localpass, sockfile))
        return results
