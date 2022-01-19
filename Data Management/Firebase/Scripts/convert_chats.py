import pandas as pd
import json
import sys

infile=sys.argv[1]
outfile=sys.argv[2]
df=pd.read_csv(infile,sep="\t",lineterminator="\n",header=None)
df.columns=['Time','Person','Message']
df=df.replace({r'\r':''},regex=True)
df=df.replace({'Person':{r':':''}},regex=True)

result=df.to_json(orient="records")
parsed=json.loads(result)

with open(outfile, 'w') as json_file:
  json.dump(parsed, json_file)