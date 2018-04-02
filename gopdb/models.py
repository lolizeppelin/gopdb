import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext import declarative


from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.dialects.mysql import SMALLINT
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.dialects.mysql import BLOB
from sqlalchemy.dialects.mysql import BOOLEAN

from simpleservice.ormdb.models import TableBase
from simpleservice.ormdb.models import InnoDBTableBase

from goperation.manager import common as manager_common
from gopdb import common

TableBase = declarative.declarative_base(cls=TableBase)


class SchemaQuote(TableBase):
    quote_id = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True, autoincrement=True)
    schema_id = sa.Column(sa.ForeignKey('gopschemas.schema_id', ondelete="CASCADE", onupdate='RESTRICT'),
                          nullable=False)
    qdatabase_id = sa.Column(sa.ForeignKey('gopdatabases.database_id', ondelete="RESTRICT", onupdate='RESTRICT'),
                            nullable=False)
    entity = sa.Column(INTEGER(unsigned=True), nullable=True)
    endpoint = sa.Column(VARCHAR(manager_common.MAX_ENDPOINT_NAME_SIZE),
                         nullable=True)
    desc = sa.Column(VARCHAR(256), nullable=True)
    __table_args__ = (
        sa.Index('schema_index', schema_id),
        sa.Index('databaseindex', qdatabase_id),
        sa.Index('entity_index', entity, endpoint),
        InnoDBTableBase.__table_args__
    )


class GopSchema(TableBase):
    schema_id = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True, autoincrement=True)
    schema = sa.Column(VARCHAR(64), nullable=False)
    database_id = sa.Column(sa.ForeignKey('gopdatabases.database_id', ondelete="RESTRICT", onupdate='RESTRICT'),
                            nullable=False)
    user = sa.Column(VARCHAR(64), default=None, nullable=False)
    passwd = sa.Column(VARCHAR(128), default=None, nullable=False)
    ro_user = sa.Column(VARCHAR(64), default=None, nullable=False)
    ro_passwd = sa.Column(VARCHAR(128), default=None, nullable=False)
    source = sa.Column(VARCHAR(64), default=None, nullable=False)
    rosource = sa.Column(VARCHAR(64), default=None, nullable=False)
    character_set = sa.Column(VARCHAR(64), default=None, nullable=True)
    collation_type = sa.Column(VARCHAR(64), default=None, nullable=True)
    desc = sa.Column(VARCHAR(256), nullable=True)
    quotes = orm.relationship(SchemaQuote, backref='schema',
                              lazy='select', cascade='delete,delete-orphan')
    __table_args__ = (
        sa.UniqueConstraint('database_id', 'schema', name='unique_schema'),
        InnoDBTableBase.__table_args__
    )


class GopSalveRelation(TableBase):
    master_id = sa.Column(sa.ForeignKey('gopdatabases.database_id', ondelete="RESTRICT", onupdate='RESTRICT'),
                          primary_key=True)
    slave_id = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True)
    readonly = sa.Column(BOOLEAN, nullable=False, default=True)
    __table_args__ = (
        InnoDBTableBase.__table_args__
    )


class GopDatabase(TableBase):
    database_id = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True,
                            autoincrement=True)
    #  local, record, cloud.aliyun, cloud.qcloud
    impl = sa.Column(VARCHAR(64), default=None, nullable=False)
    # database type, mysql or redis
    dbtype = sa.Column(VARCHAR(64), default='mysql', nullable=False)
    dbversion = sa.Column(VARCHAR(64), default=None, nullable=True)
    reflection_id = sa.Column(VARCHAR(128), nullable=False)
    user = sa.Column(VARCHAR(64), default=None, nullable=False)
    # passwd none means database can not be control
    passwd = sa.Column(VARCHAR(128), default=None, nullable=True)
    status = sa.Column(TINYINT, default=common.UNACTIVE, nullable=False)
    # bitwise operation for affinity
    affinity = sa.Column(TINYINT, default=0, nullable=False)
    desc = sa.Column(VARCHAR(256), nullable=True)
    is_master = sa.Column(BOOLEAN, nullable=False, default=True)
    slaves = orm.relationship(GopSalveRelation, lazy='select',
                              cascade='delete,delete-orphan')
    schemas = orm.relationship(GopSchema, backref='database', lazy='select',
                               cascade='delete,delete-orphan')
    quotes = orm.relationship(SchemaQuote, backref='database',
                              lazy='select', cascade='delete,delete-orphan')
    __table_args__ = (
        sa.Index('impl_index', impl),
        sa.UniqueConstraint('impl', 'reflection_id', name='unique_reflection'),
        InnoDBTableBase.__table_args__
    )


class RecordDatabase(TableBase):
    record_id = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True, autoincrement=True)
    zone = sa.Column(VARCHAR(32), nullable=False)
    host = sa.Column(VARCHAR(200), default=None, nullable=False)
    port = sa.Column(SMALLINT(unsigned=True), default=3306, nullable=False)
    extinfo = sa.Column(BLOB, nullable=True, default=None)
    __table_args__ = (
        sa.Index('izone_index', zone),
        sa.UniqueConstraint('host', 'port', name='unique_record'),
        InnoDBTableBase.__table_args__
    )


class CloudDatabase(TableBase):
    intance_id = sa.Column(VARCHAR(128), nullable=False, primary_key=True)
    zone = sa.Column(VARCHAR(32), nullable=False)
    host = sa.Column(VARCHAR(200), default=None, nullable=False)
    port = sa.Column(SMALLINT(unsigned=True), default=3306, nullable=False)
    extinfo = sa.Column(BLOB, nullable=True, default=None)
    __table_args__ = (
        sa.Index('izone_index', zone),
        sa.UniqueConstraint('host', 'port', name='unique_intance'),
        InnoDBTableBase.__table_args__
    )
