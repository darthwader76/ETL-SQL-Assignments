import sqlite3
from sqlite3 import Error
import json


json_string = """
{
	"totalResults":4,
	"columnHeaders": [
		{
			"name":"company_id",
			"dataType": "STRING"
		},
		{
			"name":"plan_started_on",
			"dataType": "STRING"
		},
		{
			"name":"plan_type",
			"dataType": "STRING"
		}
	],
	"results":[
		["1","20170101","free trail"],
		["1","20170103","advanced"],
		["2","20170214","free trail"],
		["2","20170228","free"]
	]	
}
"""

data = json.loads(json_string)
columnHeaders = data["columnHeaders"]
results = data ["results"]
create_table_string = "CREATE TABLE IF NOT EXISTS SAAS_history ("
for column in columnHeaders:
	create_table_string += str(column["name"])+" "+str(column["dataType"])+ ", "

create_table_string = create_table_string[:-2]+");"
#print(create_table_string)

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
def create_connection(db_file, create_table_string, results, columnHeaders):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        #print(sqlite3.version)
        if conn is not None:
        	create_table(conn, create_table_string)
        	for r in results:
        		create_tuple(conn, tuple(r), columnHeaders)
        	c = conn.cursor()
        	c.execute("Select * from SAAS_history;")
        	rows = c.fetchall()
        	for r in rows:
        		print(r)
        else:
        	print("Error")
    except Error as e:
        print(e)
    
def create_tuple(conn, result, columnHeaders):
 	insert = "INSERT INTO SAAS_history("
 	for column in columnHeaders:
 		insert +=str(column["name"]) + ","
 	insert = insert[:-1]+") VALUES(?,?,?);"
 	#print(insert)
 	c = conn.cursor()
 	c.execute(insert, result)
 	print(c.lastrowid)

if __name__ == '__main__':
	
    create_connection("C:\sqlite\pythondb.db", create_table_string, results, columnHeaders)