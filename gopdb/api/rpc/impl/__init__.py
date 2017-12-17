import six
import abc



@six.add_metaclass(abc.ABCMeta)
class DatabaseManagerBase(object):

    @abc.abstractmethod
    def status(self, config, **kwargs):
        pass

    @abc.abstractmethod
    def start(self, config, **kwargs):
        pass

    @abc.abstractmethod
    def stop(self, config, **kwargs):
        pass

    @abc.abstractmethod
    def install(self, config, **kwargs):
        pass

    @abc.abstractmethod
    def dump(self, config, **kwargs):
        pass

    @abc.abstractmethod
    def prepare_conf(self, config, **kwargs):
        """"""