import six
import os
import eventlet
import ConfigParser

import psutil

from simpleutil.log import log as logging
from simpleutil.config import cfg
from simpleutil.utils import systemutils

import goperation

from gopdb import common
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


def default_config():
    cf = ConfigParser.ConfigParser()

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


class MysqlConfig(DatabaseConfigBase):

    def __init__(self, config):
        # default opts
        if not isinstance(config, ConfigParser.ConfigParser):
            raise TypeError('mysql config not ConfigParser')
        self.config = config

    @classmethod
    def load(cls, cfgfile):
        """load config from config file"""
        config = ConfigParser.ConfigParser()
        config.read(cfgfile)
        return cls(config)

    @classmethod
    def loads(cls, **kwargs):
        """load config from kwargs"""
        datadir = kwargs.pop('datadir')
        runuser = kwargs.pop('runuser')
        pidfile = kwargs.pop('pidfile')
        sockfile = kwargs.pop('sockfile')
        logfile = kwargs.pop('logfile')
        # binlog on/off
        binlog = (kwargs.pop('binlog', None) or kwargs.pop('log-bin', None))
        # init mysql default config
        config = default_config()
        # set mysqld_safe opts
        config.set('mysqld_safe', 'log-error', logfile)
        # read opts from kwargs
        for k, v in six.iteritems(kwargs):
            config.set('mysqld', k, v)
        # set default opts
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
        config = ConfigParser.ConfigParser()
        config.read(cfgfile)
        self.config = config


class DatabaseManager(DatabaseManagerBase):

    config_cls = MysqlConfig

    base_opts = ['--skip-name-resolve']

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
                # close all fd
                p = psutil.Process()
                fds = [opf.fd for opf in p.open_files()]
                for fd in fds:
                    if fd > 2:
                        os.close(fd)
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

    def install(self, cfgfile, postrun, timeout, **kwargs):
        """create database intance"""
        if not os.path.exists(cfgfile):
            raise
        args = [SH, MYSQLINSTALL, '--defaults-file=%s' % cfgfile]
        args.extend(self.base_opts)
        logfile = kwargs.get('logfile')
        if not systemutils.POSIX:
            # just for test on windows
            LOG.info('will call %s', ' '.join(args))
        else:
            with goperation.tlock('gopdb-install', 30):
                pid = safe_fork()
                if pid == 0:
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
        if postrun:
            postrun()

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

