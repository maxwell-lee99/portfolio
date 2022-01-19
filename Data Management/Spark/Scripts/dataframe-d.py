from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HW5").getOrCreate()

countrylanguage=spark.read.json('countrylanguage.json')

countrylanguage.filter("countrycode = 'CAN'").select(countrylanguage['language']).show()