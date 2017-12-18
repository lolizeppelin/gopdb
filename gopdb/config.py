from simpleutil.config import cfg
def list_server_opts():
    from simpleservice.ormdb.config import database_opts
    from goperation.manager.wsgi.config import route_opts
    cfg.set_defaults(route_opts, routes=['gopdb.api.wsgi.routers'])
    return route_opts + database_opts


def list_agent_opts():
    from gopdb import common
    group = cfg.OptGroup(common.DB)
    CONF = cfg.CONF
    CONF.register_group(group)
    from goperation.manager.rpc.agent.config import rpc_endpoint_opts
    from gopdb.api.rpc.impl.config import db_opts
    from gopdb.api.rpc.impl.mysql.config import mysql_db_opts
    cfg.set_defaults(rpc_endpoint_opts, module='gopdb.api.rpc')
    return rpc_endpoint_opts + db_opts + mysql_db_opts
