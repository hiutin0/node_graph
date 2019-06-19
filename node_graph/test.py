# from utils.api_operation import *
#
# ip = '54.147.255.148'
# a = get_location_ip(ip)
# print(a)


#!/usr/bin/python3

hostname = 'localhost'
username = 'aaronyu'
password = 'PASSWORD'
database = 'DBNAME' # DBNAME

create_database = "CREATE TABLE conditions (" \
                  "time    TIMESTAMPTZ NOT NULL, " \
                  "location  TEXT    NOT NULL, " \
                  "temperature   DOUBLE  PRECISION   NULL, humidity DOUBLE  PRECISION   NULL" \
                  ");"
# Simple routine to run a query on a database and print the results:
def doQuery(conn):
    cur = conn.cursor()

    # cur.execute("INSERT INTO employee (fname, lname) VALUES ('FDFDd', 'FFD')")
    cur.execute("SELECT fname, lname FROM employee")

    for firstname, lastname in cur.fetchall():
        print(firstname, lastname)


print("Using psycopg2 ...")
import psycopg2
myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
doQuery(myConnection)
myConnection.commit()
myConnection.close()

print(create_database)

# print("Using PyGreSQL…")
# import pgdb
# myConnection = pgdb.connect( host=hostname, user=username, password=password, database=database )
# doQuery( myConnection )
# myConnection.close()