import pandas as pd
import json
import sys

infile=sys.argv[1]
outfile=sys.argv[2]

roster=pd.read_csv(infile)
roster['LName'], roster['FName']=roster['Name'].str.split(',',1).str
roster.FName = roster.FName.str.lstrip()
roster['Name']=roster['FName'].str.cat(roster['LName'],sep=' ')
roster.drop(columns=['FName','LName'],inplace=True)

result=roster.to_json(orient="records")
parsed=json.loads(result)

with open(outfile, 'w') as json_file:
  json.dump(parsed, json_file)