import pandas as pd
import json
import sys
import requests as rq

infile=sys.argv[1]
lower=infile.lower()
name=infile.lower().replace(' ','%20')
url='https://dsci-551-test-max-lee-default-rtdb.firebaseio.com/chats.json?orderBy="$key"&equalTo="{}"'.format(name)
response=rq.get(url)
if len(response.json())>0:
    for x in range(len(response.json()[lower])):
        print(response.json()[lower][x]['Time'],'   ',response.json()[lower][x]['Message'])



