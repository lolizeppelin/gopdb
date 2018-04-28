DB = 'gopdb'

ENDPOINTKEY = '%s.endpoint' % DB

ALLPRIVILEGES = 'ALL'
READONLYPRIVILEGES = 'SELECT'
REPLICATIONRIVILEGES = 'REPLICATION SLAVE'

UNACTIVE = -1
OK = 0

VERSIONMAP = {}

IGNORES = {'mysql': frozenset(['information_schema', 'performance_schema', 'sys', 'mysql'])}
