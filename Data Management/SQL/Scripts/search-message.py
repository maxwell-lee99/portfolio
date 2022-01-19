import sys
import pandas as pd
import json
import mysql.connector

infile=sys.argv[1]
cnx = mysql.connector.connect(user='dsci551',password='Dsci-551',host='localhost',database='dsci551')
cursor=cnx.cursor()

query=('''select time, text from chat where name like %s''')
data=(infile,)
cursor.execute(query,data)

for i in cursor:
    print(i[0],'    ',i[1])