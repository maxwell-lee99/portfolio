import pandas as pd
import json
import sys
import requests as rq

infile1=sys.argv[1]
infile2=sys.argv[2]

with open(infile1) as f:
    chats=json.load(f)

with open(infile2) as f:
    roster=json.load(f)




roster_names=[]
for i in range(len(roster)):
    x=roster[i]['Name'].lower().split()
    for z in x:
        roster_names.append(z)
list_set=set(roster_names)
unique_names=(list(list_set))

dic={}
for i in unique_names:
    lis=[]
    for x in range(len(roster)):
        if i in list(roster[x]['Name'].lower().split()):
            lis.append(roster[x])
    dic[i]=lis
roster=json.dumps(dic)


chats_names=[chats[i]['Person'].lower() for i in range(len(chats))]
list_set=set(chats_names)
unique_names=(list(list_set))
dic={}
for i in unique_names:
    lis=[]
    for x in range(len(chats)):
        if chats[x]['Person'].lower()==i:
            lis.append(chats[x])
    dic[i]=lis

chats=json.dumps(dic)


url='https://dsci-551-test-max-lee-default-rtdb.firebaseio.com/chats.json'
response=rq.put(url,data=chats)
url='https://dsci-551-test-max-lee-default-rtdb.firebaseio.com/roster.json'
response=rq.put(url,data=roster)
