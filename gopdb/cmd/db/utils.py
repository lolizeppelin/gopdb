from simpleservice.ormdb.tools.utils import init_database

from gopdb.models import TableBase
from gopdb.models import GopDatabase
from gopdb.models import RecordDatabase
from gopdb.models import GopSchema
from gopdb.models import SchemaQuote


def init_gopdb(db_info):

    def func(engine):
        pass
        # engine.execute("ALTER TABLE %s AUTO_INCREMENT = 1" % GopDatabase.__tablename__)
        # engine.execute("ALTER TABLE %s AUTO_INCREMENT = 1" % RecordDatabase.__tablename__)
        # engine.execute("ALTER TABLE %s AUTO_INCREMENT = 1" % GopSchema.__tablename__)
        # engine.execute("ALTER TABLE %s AUTO_INCREMENT = 1" % SchemaQuote.__tablename__)

    init_database(db_info, TableBase.metadata, init_data_func=func)
