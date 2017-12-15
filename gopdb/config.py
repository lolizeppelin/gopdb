from simpleutil.config import cfg
def list_server_opts():
    from simpleservice.ormdb.config import database_opts
    from goperation.manager.wsgi.config import route_opts
    cfg.set_defaults(route_opts, routes=['gopdb.api.wsgi.routers'])
    return route_opts + database_opts


def list_agent_opts():
    from goperation.manager.rpc.agent.config import rpc_endpoint_opts
    cfg.set_defaults(rpc_endpoint_opts, module='gopdb.api.rpc')
    return rpc_endpoint_opts
