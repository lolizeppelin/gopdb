import time
import simpleservice

from simpleutil.config import cfg
from goperation import config

from goperation.api.client import ManagerClient

from gopdb.api.client import GopDBClient
from gopdb import common


a = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\goperation.conf'
b = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\gcenter.conf'
config.configure('test', [a, b])

wsgi_url = '127.0.0.1'
# wsgi_url = '172.31.0.110'
wsgi_port = 7999

httpclient = ManagerClient(wsgi_url, wsgi_port)

client = GopDBClient(httpclient)


def create_test():
    print client.databases_create(body={'impl': 'record',
                                        'user': 'root',
                                        'dbtype': 'mysql',
                                        'passwd': None,
                                        'host': '127.0.0.1',
                                        'port': 3307})


def create_local_test():
    print client.databases_create(body={'impl': 'local',
                                        'agent_id': 1,
                                        'dbtype': 'mysql',
                                        'user': 'root',
                                        'passwd': 111111})

def index_test():
    print client.databases_index(body={})



def show_test():
    print client.database_show(database_id=1, body={})


def delete_test():
    print client.database_delete(database_id=1, body={})


def schema_create_test(database_id):
    print client.schemas_create(database_id=database_id,
                                body={'auth': {'user': 'root', 'passwd': '1111',
                                               'ro_user': 'selecter', 'ro_passwd': '111',
                                               'source': '%'},
                                      'options': {'charcter_set': 'utf8'},
                                      'name': 'gamserver_db_3'})

def schema_delete_test(database_id):
    print client.schemas_delete(database_id=database_id, schema='gamserver_db_2')


def schema_bond(database_id):
    print client.schemas_bond(database_id, schema='gamserver_db_2', entity=1, endpoint='mszl',
                              body={'esure': False})


def quote_show(quote_id, body=None):
    print client.quote_show(quote_id, body)

# index_test()
# schema_delete_test(3)
# quote_show(quote_id=1, body={'schema': True, 'database': True})
create_local_test()