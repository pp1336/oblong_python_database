import pg

class Database_connector:

    _database
    isOpen

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


    
