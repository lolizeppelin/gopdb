import six
import abc

from simpleutil.config import cfg

from gopdb import common
from gopdb.api.rpc.impl import config

CONF = cfg.CONF

config.register_opts(CONF.find_group(common.DB))


@six.add_metaclass(abc.ABCMeta)
class DatabaseConfigBase(object):

    @classmethod
    def load(cls, cfgfile):
        """load config from config file"""

    def save(self, cfgfile):
        """save config"""

    def dump(self, cfgfile):
        """out put config"""

    def update(self, cfgfile):
        """update config"""


@six.add_metaclass(abc.ABCMeta)
class DatabaseManagerBase(object):

    config_cls = None

    @abc.abstractmethod
    def status(self, cfgfile, **kwargs):
        """status of database intance"""

    @abc.abstractmethod
    def start(self, cfgfile, postrun, timeout, **kwargs):
        """stary database intance"""

    @abc.abstractmethod
    def stop(self, cfgfile, postrun, timeout, **kwargs):
        """stop database intance"""

    @abc.abstractmethod
    def install(self, cfgfile, postrun, timeout, **kwargs):
        """create database intance"""

    @abc.abstractmethod
    def dump(self, cfgfile, postrun, timeout, **kwargs):
        """dump database data"""

    @abc.abstractmethod
    def load_conf(self, cfgfile, **kwargs):
        """out put database config"""

    @abc.abstractmethod
    def save_conf(self, cfgfile, **kwargs):
        """update database config"""
