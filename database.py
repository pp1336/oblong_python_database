import pg

class Database_connector:

    _database

    def __init__(self, dbname, host, user):
        _database = pg.connect(dbname=dbname, host=host, user=user)

    def _query_(query):
        return (_database.query(query)).dictresult()


    
