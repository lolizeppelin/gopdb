from goperation.manager.exceptions import AcceptableError
from goperation.manager.exceptions import UnAcceptableError

class GopdbError(Exception):
    """gopdb base error"""


class GopdbDatabaseError(Exception):
    """gopdb database base error"""


class GopdbSchemaError(Exception):
    """gopdb schema base error"""


class AcceptableDbError(GopdbDatabaseError, AcceptableError):
    """gopdb database Acceptable error"""


class UnAcceptableDbError(GopdbDatabaseError, UnAcceptableError):
    """gopdb database UnAcceptable error"""


class AcceptableSchemaError(GopdbSchemaError, AcceptableError):
    """gopdb schema Acceptable error"""


class UnAcceptableSchemaError(GopdbSchemaError, UnAcceptableError):
    """gopdb schema UnAcceptable error"""