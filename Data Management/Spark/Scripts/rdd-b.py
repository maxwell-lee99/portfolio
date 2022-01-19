from pyspark import SparkContext
import json
sc = SparkContext(appName="dsci551")

countries=[]
for f in open('country.json',"r"):
    countries.append((json.loads(f)['Capital'],json.loads(f)['Name']))

cities=[]
for f in open('city.json',"r"):
    cities.append((json.loads(f)['ID'],json.loads(f)['Name']))
    
cnt=sc.parallelize(countries)
cit=sc.parallelize(cities)

print(cnt.join(cit).map(lambda x: x[1]).collect())

