import sys
import pandas as pd
import json
import mysql.connector

outfile=sys.argv[1]

cnx = mysql.connector.connect(user='dsci551',password='Dsci-551',host='localhost',database='dsci551')
cursor=cnx.cursor()

query=('''select roster.name, roster.participating_from 
from roster left outer join chat on roster.name=chat.name 
where chat.name is null;''')
cursor.execute(query)

results=[]
for i in cursor:
    results.append({"Name":"{}".format(i[0]),"Participating from":"{}".format(i[1])})

cursor.close()
cnx.close()


with open(outfile, 'w') as json_file:
  json.dump(results, json_file)