import six
import abc


@six.add_metaclass(abc.ABCMeta)
class DatabaseManagerBase(object):


    def status(self):
        pass

    def start(self, configfile, **kwargs):
        pass

    def stop(self, configfile, **kwargs):
        pass

    def install(self):
        pass

    def dump(self):
        pass
