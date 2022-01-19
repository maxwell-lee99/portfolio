from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HW5").getOrCreate()

country=spark.read.json('country.json')
city=spark.read.json('city.json')

country.join(city, country.Capital == city.ID).select(country['Name'],city['Name']).show()