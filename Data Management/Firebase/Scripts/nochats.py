import pandas as pd
import json
import sys

infile1=sys.argv[1]
infile2=sys.argv[2]
outfile=sys.argv[3]
chats=pd.read_csv(infile1,sep="\t",lineterminator="\n",header=None)


chats.columns=['time','Name','Message']
chats=chats.replace({r'\r':''},regex=True)
chats=chats.replace({'Name':{r':':''}},regex=True)
chats.drop(columns=['time','Message'],inplace=True)

roster=pd.read_csv(infile2)
roster['LName'], roster['FName']=roster['Name'].str.split(',',1).str
roster.FName = roster.FName.str.lstrip()
roster['Name']=roster['FName'].str.cat(roster['LName'],sep=' ')
roster.drop(columns=['FName','LName'],inplace=True)
roster['isin']=roster['Name'].isin(list(chats['Name']))
roster=roster[roster['isin']==False]
roster.drop(columns=['isin'],inplace=True)

result=roster.to_json(orient='records')
parsed=json.loads(result)
with open(outfile, 'w') as json_file:
  json.dump(parsed, json_file)