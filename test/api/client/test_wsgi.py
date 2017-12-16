import time
import simpleservice

from simpleutil.config import cfg
from goperation import config

from goperation.api.client import ManagerClient

from gopdb.api.client import GopCdnClient
from gopdb import common


a = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\goperation.conf'
b = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\gcenter.conf'
config.configure('test', [a, b])

wsgi_url = '127.0.0.1'
# wsgi_url = '172.31.0.110'
wsgi_port = 7999

httpclient = ManagerClient(wsgi_url, wsgi_port)

client = GopCdnClient(httpclient)


def create_test():
    print client.databases_create(body={'impl': 'record',
                                        'user': 'root',
                                        'passwd': None,
                                        'host': '127.0.0.1',
                                        'port': 3306})

def index_test():
    print client.databases_index(body={})



def show_test():
    print client.database_show(database_id=1, body={})


def delete_test():
    print client.database_delete(database_id=1, body={})


create_test()