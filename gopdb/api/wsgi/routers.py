from simpleservice.wsgi import router
from simpleservice.wsgi.middleware import controller_return_response

from gopdb import common
from gopdb.api.wsgi import controller


COLLECTION_ACTIONS = ['index', 'create']
MEMBER_ACTIONS = ['show', 'update', 'delete']


class Routers(router.RoutersBase):

    def append_routers(self, mapper, routers=None):
        resource_name = 'database'
        collection_name = resource_name + 's'
        db_controller = controller_return_response(controller.DatabaseReuest(),
                                                   controller.FAULT_MAP)

        self._add_resource(mapper, db_controller,
                           path='/%s/select' % common.DB,
                           get_action='select')

        self._add_resource(mapper, db_controller,
                           path='/%s/{impl}/reflect' % common.DB,
                           get_action='reflect')

        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=db_controller,
                                       path_prefix='/%s' % common.DB,
                                       member_prefix='/{database_id}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)
        collection.member.link('slaves', method='GET')
        collection.member.link('start', method='POST')
        collection.member.link('stop', method='POST')
        collection.member.link('status', method='GET')


        resource_name = 'schema'
        collection_name = resource_name + 's'
        schema_controller = controller_return_response(controller.SchemaReuest(),
                                                       controller.FAULT_MAP)

        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=schema_controller,
                                       path_prefix='/%s/database/{database_id}' % common.DB,
                                       member_prefix='/{schema}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)
        collection.member.link('copy', method='POST')
        collection.member.link('bond', method='POST')

        self._add_resource(mapper, schema_controller,
                           path='/%s/quotes/{quote_id}' % common.DB,
                           get_action='quote')

        self._add_resource(mapper, schema_controller,
                           path='/%s/quotes/{quote_id}' % common.DB,
                           delete_action='unquote')
