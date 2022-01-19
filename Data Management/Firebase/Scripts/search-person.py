import pandas as pd
import json
import sys
import requests as rq

infile=sys.argv[1]
names=[]
splt=infile.lower().split(' ')
for i in splt:
    url='https://dsci-551-test-max-lee-default-rtdb.firebaseio.com/roster.json?orderBy="$key"&equalTo="{}"'.format(i)
    response=rq.get(url)
    if len(response.json())>0:
        for x in range(len(response.json()[i])):
            print(response.json()[i][x]['Name'])

list_set=set(names)
unique_names=(list(list_set))
for x in unique_names:
    print(x)
