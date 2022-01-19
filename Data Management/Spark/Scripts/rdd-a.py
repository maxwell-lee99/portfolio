from pyspark import SparkContext
import json
sc = SparkContext(appName="dsci551")

countries=[]
for f in open('country.json',"r"):
    countries.append(json.loads(f))

print(sc.parallelize(countries).filter(lambda x: x['Continent']=='North America').map(lambda x: x['Name']).collect())
