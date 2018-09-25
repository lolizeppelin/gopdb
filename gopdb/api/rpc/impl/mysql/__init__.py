import six
import os
import contextlib
import eventlet
import ConfigParser
from collections import OrderedDict
import psutil
import mysql.connector

from simpleutil.log import log as logging
from simpleutil.config import cfg
from simpleutil.utils import systemutils

import goperation

from gopdb import common
from gopdb import privilegeutils
from gopdb.api import exceptions
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


class MultiOrderedDict(OrderedDict):
    def __setitem__(self, key, value,
                    dict_setitem=dict.__setitem__):
        if key in MULTIABLEOPTS and key in self:
            if isinstance(self[key], list):
                if value not in self[key]:
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
                            fp.write("%s\n" % line)
                    else:
                        key = " = ".join((key, str(value).replace('\n', '\n\t')))
                        fp.write("%s\n" % key)
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
        try:
            return self.config.get('mysqld', key)
        except ConfigParser.NoOptionError:
            return None

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

    def _slave_status(self, conn):
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SHOW ALL SLAVES STATUS')
        slaves = cursor.fetchall()
        cursor.close()
        return slaves

    def _master_status(self, conn):
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SHOW MASTER STATUS')
        masters = cursor.fetchall()
        cursor.close()
        return masters[0] if masters else None

    def _schemas(self, conn):
        schemas = []
        cursor = conn.cursor()
        cursor.execute('SHOW DATABASES')
        for result in cursor.fetchall():
            schema = result[0]
            if schema not in common.IGNORES['mysql']:
                schemas.append(schema)
        cursor.close()
        return schemas

    def _binlog_on(self, conn):
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SHOW GLOBAL VARIABLES LIKE 'log_bin'")
        varinfo = cursor.fetchall()[0]
        cursor.close()
        if varinfo.get('Value').lower() == 'on':
            return True
        return False

    @contextlib.contextmanager
    def _lower_conn(self, sockfile, user, passwd, schema=None,
                    raise_on_warnings=True):
        kwargs = dict(user=user, passwd=passwd, unix_socket=sockfile,
                      raise_on_warnings=raise_on_warnings)
        if schema:
            kwargs['database'] = schema
        conn = mysql.connector.connect(**kwargs)
        try:
            yield conn
        except Exception as e:
            LOG.error('lower mysql connect error %s' % e.__class__.__name__)
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.exception('mysql.connector exec error')
            raise
        finally:
            conn.close()

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
                try:
                    os.execv(SH, args)
                except OSError:
                    os._exit(1)
            else:
                os._exit(0)
        else:
            wait(pid)
            eventlet.sleep(1)

    def stop(self, cfgfile, postrun=None, timeout=None, **kwargs):
        """stop database intance"""
        process = kwargs.pop('process', None)
        timeout = timeout or 3
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
        while timeout > 0:
            eventlet.sleep(1)
            timeout -= 1
            if not process.is_running():
                LOG.debug('Stop mysql process success')
                return
        raise exceptions.GopdbError('Process is running after stop')

    def bond(self, cfgfile, postrun, timeout, dbinfo,
             **kwargs):
        """slave bond to master database"""
        conf = CONF[common.DB]
        master = kwargs.pop('master')
        schemas = set(master.pop('schemas'))
        force = kwargs.pop('force', False)
        config = self.config_cls.load(cfgfile)
        sockfile = config.get('socket')

        auth = privilegeutils.mysql_slave_replprivileges(slave_id=dbinfo.get('database_id'), **master)
        master_name = 'masterdb-%(database_id)s' % auth
        sql = "CHANGE MASTER 'masterdb-%(database_id)s' TO MASTER_HOST='%(host)s', MASTER_PORT=%(port)d," \
              "MASTER_USER='%(user)s',MASTER_PASSWORD='%(passwd)s'," \
              "MASTER_LOG_FILE='%(file)s',MASTER_LOG_POS=%(position)s" % auth
        LOG.info('Replication connect sql %s' % sql)

        with self._lower_conn(sockfile, conf.localroot, conf.localpass) as conn:
            LOG.info('Login mysql from unix sock %s success, try bond master' % sockfile)

            if schemas & set(self._schemas(conn)):
                raise exceptions.AcceptableSchemaError('Schema with same name exist')
            LOG.info('Slave channel name %s' % master_name)
            slaves = self._slave_status(conn)
            for slave_status in slaves:
                channel = slave_status.get('Connection_name')
                host = slave_status.get('Master_Host')
                port = slave_status.get('Master_Port')
                iothread = slave_status.get('Slave_IO_Running').lower()
                epos = slave_status.get('Exec_Master_Log_Pos')
                rpos = slave_status.get('Read_Master_Log_Pos')
                bsecond = slave_status.get('Seconds_Behind_Master')
                if channel != master_name and (host == auth.get('host') and port == auth.get('port')):
                    LOG.info('Bond slave find same host and port with different channel name %s' % channel)
                    if iothread == 'yes':
                        raise exceptions.AcceptableDbError('Slave with channel name %s '
                                                           'is running in same host:port' % channel)
                    if epos != 0 or rpos != 0 or bsecond != 0:
                        if not force:
                            raise exceptions.AcceptableDbError('Channel %s pos not zero, need force' %
                                                               channel)
                    LOG.warning('Reset slave channel %s' % channel)
                    if LOG.isEnabledFor(logging.DEBUG):
                        for key in slave_status.keys():
                            LOG.debug('BOND FIND OLD SLAVE %s STATUS ------ %s : %s'
                                      % (channel, key, slave_status[key]))
                    cursor = conn.cursor()
                    cursor.execute("RESET SLAVE '%s' ALL" % channel)
                    cursor.close()
                    break
                elif channel == master_name:
                    LOG.info('Bond slave find same channel')
                    if host != auth.get('host') or port != auth.get('port'):
                        if iothread == 'yes':
                            raise exceptions.AcceptableDbError('Channel %s is running but '
                                                               'connection is not the same' % channel)
                    if epos != 0 or rpos != 0 or bsecond != 0:
                        if not force:
                            raise exceptions.AcceptableDbError('Channel %s pos not zero, need force' %
                                                               channel)
                    if LOG.isEnabledFor(logging.DEBUG):
                        for key in slave_status.keys():
                            LOG.debug('BOND FIND OLD SLAVE %s STATUS ------ %s : %s'
                                      % (channel, key, slave_status[key]))
                    if iothread == 'yes':
                        cursor = conn.cursor()
                        cursor.execute("STOP SLAVE '%s'" % channel)
                        cursor.close()
                    cursor = conn.cursor()
                    cursor.execute("RESET SLAVE '%s' ALL" % channel)
                    cursor.close()
                    break

            cursor = conn.cursor()
            cursor.execute(sql)
            cursor.close()

            LOG.info('Connect to master success, try start slave')
            # master have no schemas auto start slave
            if not schemas:
                cursor = conn.cursor()
                cursor.execute("START SLAVE '%s'" % master_name)
                cursor.close()
                LOG.info('START SLAVE %s success' % master_name)

        if postrun:
            postrun()

    def unbond(self, cfgfile, postrun, timeout,
               **kwargs):
        """slave unbond master database"""
        conf = CONF[common.DB]
        master = kwargs.pop('master')
        schemas = master.get('schemas')
        ready = master.get('ready')
        force = kwargs.pop('force', False)
        config = self.config_cls.load(cfgfile)
        sockfile = config.get('socket')

        master_name = 'masterdb-%(database_id)s' % master

        with self._lower_conn(sockfile, conf.localroot, conf.localpass,
                              schema=None, raise_on_warnings=False) as conn:
            LOG.info('Login mysql from unix sock success, try stop salve and unbond')
            # check schemas
            if (set(schemas) - set(self._schemas(conn))) and not force:
                raise exceptions.AcceptableDbError('Slave schemas not same as master %(database_id)s' % master)
            slaves = self._slave_status(conn)
            for slave_status in slaves:
                if slave_status.get('Connection_name') == master_name:
                    if LOG.isEnabledFor(logging.DEBUG):
                        for key in slave_status.keys():
                            LOG.debug('UNBOND SLAVE %s STATUS ------ %s : %s'
                                      % (master_name, key, slave_status[key]))
                    running = False
                    if slave_status.get('Slave_IO_Running').lower() == 'yes' \
                            or slave_status.get('Slave_SQL_Running').lower() == 'yes':
                        running = True

                    if running:
                        if ready and not force:
                            raise exceptions.AcceptableDbError('Slave thread is running')

                        cursor = conn.cursor()
                        cursor.execute("STOP SLAVE '%s'" % master_name)
                        cursor.close()

                    cursor = conn.cursor()
                    cursor.execute("RESET SLAVE '%s' ALL" % master_name)
                    cursor.close()
                    break
        if postrun:
            postrun()

    def revoke(self, cfgfile, postrun, timeout,
               **kwargs):
        """revoke from master database intance"""
        conf = CONF[common.DB]
        auth = kwargs.pop('auth')
        config = self.config_cls.load(cfgfile)
        sockfile = config.get('socket')
        if not auth.get('schema'):
            auth['schema'] = '*'

        sqls = []
        sqls.append("REVOKE %(privileges)s ON %(schema)s.* FROM '%(user)s'@'%(source)s'" % auth)
        sqls.append("DROP USER '%(user)s'@'%(source)s'" % auth)
        sqls.append("FLUSH PRIVILEGES")

        with self._lower_conn(sockfile, conf.localroot, conf.localpass,
                              schema=None, raise_on_warnings=False) as conn:
            LOG.info('Login mysql from unix sock %s success, try revoke and drop user' % sockfile)
            for sql in sqls:
                cursor = conn.cursor()
                cursor.execute(sql)
                cursor.close()
        if postrun:
            postrun()

    def bondslave(self, cfgfile, postrun, timeout, dbinfo,
                  **kwargs):
        """master bond salve"""
        conf = CONF[common.DB]
        replication = kwargs.pop('replication')
        schemas = kwargs.pop('schemas')
        cf = self.config_cls.load(cfgfile)
        sockfile = cf.get('socket')
        if not cf.get('log-bin'):
            if schemas:
                raise exceptions.AcceptableDbError('Databaes bin log is off in config file')
            LOG.warning('Database log-bin not open, try open it')
            cf.binlog()
            with self._lower_conn(sockfile, conf.localroot, conf.localpass,
                                  schema=None, raise_on_warnings=False) as conn:
                if self._binlog_on(conn):
                    LOG.error('Config file %s value error on log bin' % cfgfile)
                    raise exceptions.UnAcceptableDbError('Log bin is on in mysql process but off in config')
                if self._master_status(conn):
                    raise exceptions.UnAcceptableDbError('Bin log has been opened but now closed')
            cf.save(cfgfile)
            LOG.info('log bin opened in config file, try restart mysql')
            self.stop(cfgfile, timeout=3)
            self.start(cfgfile)
            if not os.path.exists(sockfile):
                eventlet.sleep(1)

        sqls = []
        sqls.append("grant %(privileges)s on *.* to '%(user)s'@'%(source)s' IDENTIFIED by '%(passwd)s'"
                    % replication)
        sqls.append("FLUSH PRIVILEGES")
        if not schemas:
            sqls.append("RESET MASTER")
        with self._lower_conn(sockfile, conf.localroot, conf.localpass) as conn:
            LOG.info('Login mysql from unix sock %s success, try bond slave' % sockfile)
            if not self._binlog_on(conn):
                raise exceptions.AcceptableDbError('Database binlog is off')
            if set(schemas) != set(self._schemas(conn)):
                raise exceptions.UnAcceptableDbError('Master schemas record not the same with schemas in entity')
            if schemas:
                if not kwargs.get('file') or not kwargs.get('position'):
                    raise exceptions.AcceptableSchemaError('Database got schemas, need file and position')
                LOG.warning('Database add slave with schemas already exist')
            for sql in sqls:
                LOG.debug(sql)
                cursor = conn.cursor()
                cursor.execute(sql)
                cursor.close()
            if schemas:
                binlog = dict(File=kwargs.get('file'), Position=kwargs.get('position'))
            else:
                binlog = self._master_status(conn)
        LOG.info('Grant privileges for replication user success')
        if postrun:
            try:
                postrun(binlog, schemas)
            except Exception as e:
                LOG.error('Bond slave fail with exception %s' % e.__class__.__name__)
                sqls = []
                if not replication.get('schema'):
                    replication['schema'] = '*'
                sqls.append("REVOKE %(privileges)s ON %(schema)s.* FROM '%(user)s'@'%(source)s'" % replication)
                sqls.append("DROP USER '%(user)s'@'%(source)s'" % replication)
                sqls.append("FLUSH PRIVILEGES")
                try:
                    with self._lower_conn(sockfile, conf.localroot, conf.localpass) as conn:
                        for sql in sqls:
                            LOG.debug(sql)
                            cursor = conn.cursor()
                            cursor.execute(sql)
                            cursor.close()
                # TODO change Exception type
                except Exception:
                    LOG.error('Drop user fail')
                LOG.info('Bond fail, revoke user privilege success')
                raise e

    def replication_status(self, cfgfile, postrun, timeout, **kwargs):
        """get slave replication status"""
        conf = CONF[common.DB]
        config = self.config_cls.load(cfgfile)
        sockfile = config.get('socket')

        master = kwargs.pop('master')
        schemas = set(master.pop('schemas'))
        master_name = 'masterdb-%(database_id)s' % master

        if_success = False
        msg = 'channel name %s not found' % master_name

        with self._lower_conn(sockfile, conf.localroot, conf.localpass,
                              schema=None, raise_on_warnings=False) as conn:
            if schemas - set(self._schemas(conn)):
                msg = 'Miss schemas %s' % '|'.join(list(schemas - set(self._schemas(conn))))
            else:
                slaves = self._slave_status(conn)
                for slave_status in slaves:
                    channel = slave_status.get('Connection_name')
                    host = slave_status.get('Master_Host')
                    port = slave_status.get('Master_Port')
                    if channel == master_name:
                        if host != master.get('host') or port != master.get('port'):
                            msg = 'Channel name match, but host or port not match'
                            break
                        if slave_status.get('Slave_IO_Running').lower() != 'yes' \
                                or slave_status.get('Slave_SQL_Running').lower() != 'yes':
                            msg = 'Channel find, but slave thread not running'
                            break
                        if_success = True
        if if_success:
            if postrun:
                postrun()
        else:
            LOG.debug(msg)
        return if_success, msg

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
                    try:
                        os.execv(SH, args)
                    except OSError:
                        os._exit(1)
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
        binlog = self._init_passwd(cfgfile, auth, replication)
        if postrun:
            postrun(binlog)

    def dump(self, cfgfile, postrun, timeout,
             **kwargs):
        """dump database data"""

    def load_conf(self, cfgfile, **kwargs):
        """out put database config"""

    def save_conf(self, cfgfile, **configs):
        """update database config"""
        dbconfig = self.config_cls.loads(**configs)
        dbconfig.save(cfgfile)
        systemutils.chmod(cfgfile, 0o644)

    def _init_passwd(self, cfgfile, auth, replication):
        """init password for database"""
        conf = CONF[common.DB]
        config = self.config_cls.load(cfgfile)
        sockfile = config.get('socket')

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

        sqls.append('FLUSH PRIVILEGES')
        if replication:
            sqls.append('RESET MASTER')

        with self._lower_conn(sockfile=sockfile,
                              user='root', passwd='', schema='mysql') as conn:
            LOG.info('Login mysql from unix sock %s success, try init database' % sockfile)
            for sql in sqls:
                LOG.debug(sql)
                cursor = conn.cursor()
                cursor.execute(sql)
                cursor.close()
            LOG.info('Init privileges finishd')
            binlog = None
            if replication:
                binlog = self._master_status(conn)

        return binlog
