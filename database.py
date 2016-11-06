import pg

class Database_manager:

    _create_table = "CREATE TABLE"
    _primary_key = "PRIMARY KEY"
    _from = "FROM"
    _where = "WHERE"
    _space = " "
    _semi_colon = ";"
    _comma = ","
    _left_p = "("
    _right_p = ")"
    _format_3 = "%s%s%s"
    _format_4 = "%s%s%s%s"
    _format_5 = "%s%s%s%s%s"
    _format_6 = "%s%s%s%s%s%s"
    _format_7 = "%s%s%s%s%s%s%s"

    def __init__(self, dbname, host, port, user, passwd):
        self._database = pg.connect(dbname=dbname, host=host, port=port
                                    user=user, passwd = passwd)
        self.dbname = dbname
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.isOpen = True

    def close(self, query):
        self._database.close()
        self.isOpen = False

    def _query(self, query):
        return (self._database.query(query)).dictresult()

    def _update(self, query):
        self._database.query(query)

    def create_table(self, name, cols, primary):
        cmd = Database_manager._format_4 % (Database_manager._create_table,
                                            Database_manager._space,
                                            name,
                                            Database_manager._left_p)
        l1 = len(cols)
        l2 = len(primary)
        count = 0
        for (col, t) in cols:
            if (l2 > 0 or count < l1 - 1):
                cmd = Database_manager._format_6 % (cmd,
                                                    Database_manager._space,
                                                    col,
                                                    Database_manager._space,
                                                    t,
                                                    Database_manager._comma)
                count += 1
            else:
                cmd = Database_manager._format_5 % (cmd,
                                                    Database_manager._space,
                                                    col,
                                                    Database_manager._space,
                                                    t)
        if (l2 > 0):
            cmd = Database_manager._format_4 % (cmd,
                                                Database_manager._space,
                                                Database_manager._primary_key,
                                                Database_manager._left_p)
            count = 0
            for k in primary:
                if (count < l2 - 1):
                    cmd = Database_manager._format_4 % (cmd,
                                                        Database_manager._space,
                                                        k,
                                                        Database_manager._comma)
                    count += 1
                else:
                    cmd = Database_manager._format_5 % (cmd,
                                                        Database_manager._space,
                                                        k,
                                                        Database_manager._space,
                                                        Database_manager._right_p)
        cmd = Database_manager._format_4 % (cmd,
                                            Database_manager._space,
                                            Database_manager._right_p,
                                            Database_manager._semi_colon)
        cols_name, cols_type = zip(*cols)
        self.cols_name = cols_name
        self.cols_type = cols_type
        self.tname = name
        self.no_cols = len(cols)
        self.primary = primary
        self._update(cmd)
        
    def search(self, conds):
        l = len(conds)
        cmd = "SELECT"
        count = 0
        for col in self.cols_name:
            if (count < self.no_cols - 1):
                cmd = Database_manager._format_4 % (cmd,
                                                    Database_manager._space,
                                                    col,
                                                    Database_manager._comma)
                count += 1
            else:
                cmd = Database_manager._format_3 % (cmd,
                                                    Database_manager._space,
                                                    col)
        cmd = Database_manager._format_7 % (cmd,
                                            Database_manager._space,
                                            Database_manager._from,
                                            Database_manager._space,
                                            self.tname,
                                            Database_manager._space,
                                            Database_manager._where)
        count = 0
        for cond in conds:
            if (count < l - 1):
                cmd = Database_manager._format_4 % (cmd,
                                                    Database_manager._space,
                                                    cond,
                                                    Database_manager._comma)
                count += 1
            else:
                cmd = Database_manager._format_4 % (cmd,
                                                    Database_manager._space,
                                                    cond,
                                                    Database_manager._semi_colon)
        return self._query(cmd)
            


    
