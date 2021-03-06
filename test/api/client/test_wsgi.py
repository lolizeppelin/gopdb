import time
import simpleservice

from simpleutil.config import cfg
from goperation import config

from goperation.api.client import ManagerClient

from gopdb.api.client import GopDBClient
from gopdb import common

from goperation.manager import common as manager_common


a = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\goperation.conf'
b = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\gcenter.conf'
config.configure('test', [a, b])

# wsgi_url = '127.0.0.1'
wsgi_url = '172.31.0.110'
wsgi_port = 7999

httpclient = ManagerClient(wsgi_url, wsgi_port, timeout=30)

client = GopDBClient(httpclient)


def active_agent(agent_id):
    print client.agent_active(agent_id, manager_common.ACTIVE)


def create_test():
    print client.databases_create(body={'impl': 'record',
                                        'user': 'root',
                                        'dbtype': 'mysql',
                                        'passwd': None,
                                        'host': '127.0.0.1',
                                        'port': 3307})


def create_local_test(agent_id):
    print client.databases_create(body={'impl': 'local',
                                        'agent_id': agent_id,
                                        'dbtype': 'mysql',
                                        'affinity': 2,
                                        'user': 'root',
                                        'passwd': '111111'})


def index_test():
    print client.databases_index(body={})


def show_test():
    print client.database_show(database_id=1)


def status_test(database_id):
    print client.database_status(database_id=database_id)


def start_test(database_id):
    print client.database_start(database_id=database_id)


def delete_test(database_id):
    print client.database_delete(database_id=database_id, body={})


def schema_create_test(database_id):
    print client.schemas_create(database_id=database_id,
                                body={'auth': {'user': 'root', 'passwd': '1111',
                                               'ro_user': 'selecter', 'ro_passwd': '111',
                                               'source': '%'},
                                      'options': {'character_set': 'utf8'},
                                      'name': 'gamserver_db_3'})


def schema_delete_test(database_id, schema, unquotes):
    print client.schemas_delete(database_id=database_id, schema=schema,
                                body={'unquotes': unquotes})


def schema_bond(database_id):
    print client.schemas_bond(database_id, schema='gamserver_db_2', entity=1, endpoint='mszl',
                              body={'esure': False})


def quote_show(quote_id, body=None):
    print client.quote_show(quote_id, body)


# active_agent(agent_id=2)

# index_test()
# delete_test(database_id=40)
# schema_delete_test(42, 'gogamechen1_gamesvr_datadb_3', unquotes=[51])
# schema_delete_test(43, 'gogamechen1_gamesvr_logdb_3', unquotes=[50])
# quote_show(quote_id=1, body={'schema': True, 'database': True})
# create_local_test(1)

status_test(database_id=1)
status_test(database_id=42)
status_test(database_id=43)
# start_test(database_id=43)
