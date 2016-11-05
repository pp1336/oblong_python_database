import pg

class Database_manager:

    _create_table = "CREATE TABLE table_name( "
    _primary_key = "PRIMARY KEY( "

    def __init__(self, dbname, host, user):
        self._database = pg.connect(dbname=dbname, host=host, user=user)
        self.isOpen = True

    def close(self, query):
        self._database.close()
        self.isOpen = False

    def _query_(self, query):
        return (self._database.query(query)).dictresult()

    def _update_(self, query):
        self._database.query(query)

    def create_table(self, cols, primary):
        cmd = Database_manager._create_table
        l1 = len(cols)
        l2 = len(primary)
        count = 0
        for (col, t) in cols:
            if (l2 > 0 or count < l1 - 1):
                cmd = "%s %s %s, " % (cmd, col, t)
                count += 1
            else:
                cmd = "%s %s %s" % (cmd, col, t)
        if (l2 > 0):
            cmd = "%s %s" % (cmd, _primary_key)
            count = 0
            for k in primary:
                if (count < l2 - 1):
                    cmd = "%s %s, " % (cmd, k)
                    count += 1
                else:
                    cmd = "%s %s )" % (cmd, k)
        cmd = "%s );" % (cmd)
        self._update_(cmd)


    
