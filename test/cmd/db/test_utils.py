from gopdb.cmd.db.utils import init_gopdb

dst = {'host': '172.20.0.3',
       'port': 3304,
       'schema': 'gopdb',
       'user': 'root',
       'passwd': '111111'}

init_gopdb(dst)