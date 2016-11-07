from database import DBm

db = DBm("test", "localhost", 5432, "tester", "805617")
db.create_table("table2", [("names", "text"), ("year", "integer")], ["names"])