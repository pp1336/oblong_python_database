from database import DBm

db = DBm("test", "localhost", 5432, "tester", "805617")
db.create_table("table2", [("names", "text"), ("year", "integer")], ["names"])
db.insert(["\'John\'", "24"])
db.update([("year", "22")], ["names=\'John\'"])
db.insert(["\'e\'", "24"])
db.insert(["\'b\'", "24"])
db.insert(["\'c\'", "24"])
db.insert(["\'d\'", "24"])
d = db.search(["names!=\'e\'", "year > 23"])
print (d)

