from simpleservice.plugin.exceptions import ServerExecuteRequestError

from goperation.api.client import GopHttpClientApi
from goperation.manager import common

# from gopdb.common import DB
from gopdb.common import ENDPOINTKEY


class GopDBClient(GopHttpClientApi):

    select_databases_path = '/gopdb/select'
    reflect_databases_path = '/gopdb/%s/reflect'
    databases_path = '/gopdb/databases'
    database_path = '/gopdb/databases/%s'
    database_path_ex = '/gopdb/databases/%s/%s'

    schemas_path = '/gopdb/database/%s/schemas'
    schema_path = '/gopdb/database/%s/schemas/%s'
    schema_path_ex = '/gopdb/database/%s/schemas/%s/%s'

    quote_path = '/gopdb/quotes/%s'

    def __init__(self, httpclient):
        # self.endpoint = DB
        super(GopDBClient, self).__init__(httpclient)

    def reflect_database(self, impl, body):
        resp, results = self.get(action=self.reflect_databases_path % impl, body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='select database fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def select_database(self, body=None):
        resp, results = self.get(action=self.select_databases_path, body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='select database fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def databases_create(self, body):
        resp, results = self.post(action=self.databases_path, body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='create database fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def databases_index(self, body):
        resp, results = self.get(action=self.databases_path, body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='get database list fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def database_show(self, database_id, body):
        resp, results = self.get(action=self.database_path % str(database_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='show database %s fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def database_update(self, database_id, body):
        resp, results = self.put(action=self.database_path % str(database_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='update database %s fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def database_delete(self, database_id, body):
        resp, results = self.delete(action=self.database_path % str(database_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='delete database %s fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def database_get_slaves(self, database_id, body):
        resp, results = self.get(action=self.database_path % (str(database_id), 'slaves'), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='get database %s slave list fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def database_get_status(self, database_id, body):
        resp, results = self.get(action=self.database_path % (str(database_id), 'status'), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='get database %s slave list fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def database_start(self, database_id, body):
        resp, results = self.retryable_post(action=self.database_path % (str(database_id), 'start'), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='get database %s slave list fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def database_stop(self, database_id, body):
        resp, results = self.post(action=self.database_path % (str(database_id), 'stop'), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='get database %s slave list fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    # schemas api

    def schemas_create(self, database_id, body):
        resp, results = self.retryable_post(action=self.schemas_path % str(database_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='create schema on %s fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def schemas_index(self, database_id, body):
        resp, results = self.get(action=self.schemas_path % str(database_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='get schemas from %s fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def schemas_show(self, database_id, schema, body):
        resp, results = self.get(action=self.schema_path % (str(database_id), schema), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='get schema info from %s fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def schemas_update(self, database_id, schema, body):
        resp, results = self.put(action=self.schema_path % (str(database_id), schema), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='update schema info from %s fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def schemas_delete(self, database_id, schema, body=None):
        resp, results = self.delete(action=self.schema_path % (str(database_id), schema), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='delete schema info from %s fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def schemas_copy(self, database_id, schema, body):
        resp, results = self.delete(action=self.schema_path_ex % (str(database_id), schema, 'copy'), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='copy schema info from %s fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def schemas_bond(self, database_id, schema, entity, endpoint, body=None):
        body = body or {}
        body.update({ENDPOINTKEY: endpoint})
        body.update({'entity': entity})
        resp, results = self.post(action=self.schema_path_ex % (str(database_id), schema, 'bond'), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='bond schema info on %s fail:%d' %
                                                    (str(database_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def quote_show(self, quote_id, body=None):
        resp, results = self.get(action=self.quote_path % str(quote_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='show quote %s info fail:%d' %
                                                    (str(quote_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def quote_unbond(self, quote_id, body):
        resp, results = self.delete(action=self.quote_path % str(quote_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='unquote %s fail:%d' %
                                                    (str(quote_id), results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results
