import mysql.connector
import pandas as pd
import sys

infile=sys.argv[1]
infile2=sys.argv[2]

cnx = mysql.connector.connect(user='dsci551',password='Dsci-551',host='localhost',database='dsci551')
cursor=cnx.cursor()

query=('''drop table if exists chat;''')
cursor.execute(query)
cnx.commit()

query=('''create table if not exists chat(
    Time varchar(255),
    Name text(500),
    Text varchar(255),
    fulltext idx (Name)) engine=InnoDB;''')
cursor.execute(query)
cnx.commit()

df=pd.read_csv(infile,sep="\t",lineterminator="\n",header=None)
df.columns=['Time','Person','Message']
df=df.replace({r'\r':''},regex=True)
df=df.replace({'Person':{r':':''}},regex=True)
for i in range(len(df)):
    time=str(df.iloc[i,0])
    name=str(df.iloc[i,1])
    chat=str(df.iloc[i,2])
    query=('''insert into chat (time, name, text) values (%s,%s,%s);''')
    data=(time,name,chat)
    cursor.execute(query,data)
    cnx.commit()


query=('''drop table if exists roster;''')
cursor.execute(query)
cnx.commit()

query=('''create table if not exists roster(
    Name text(500),
    Participating_from varchar(255),
    fulltext idx (name))
    engine=InnoDB;''')
cursor.execute(query)
cnx.commit()

roster=pd.read_csv(infile2)
roster['LName'], roster['FName']=roster['Name'].str.split(',',1).str
roster.FName = roster.FName.str.lstrip()
roster['Name']=roster['FName'].str.cat(roster['LName'],sep=' ')
roster.drop(columns=['FName','LName'],inplace=True)

for i in range(len(roster)):
    name=str(roster.iloc[i,0])
    location=str(roster.iloc[i,1])
    query=('''insert into roster (name, participating_from) values (%s,%s);''')
    data=(name,location)
    cursor.execute(query,data)
    cnx.commit()



cursor.close()
cnx.close()