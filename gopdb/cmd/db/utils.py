from simpleservice.ormdb.tools.utils import init_database

from gopdb.models import TableBase
from gopdb.models import GopDatabase


def init_gopdb(db_info):

    def func(engine):
        engine.execute("ALTER TABLE %s AUTO_INCREMENT = 1" % GopDatabase.__tablename__)
    init_database(db_info, TableBase.metadata, init_data_func=func)
