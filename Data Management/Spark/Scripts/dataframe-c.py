from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HW5").getOrCreate()

country=spark.read.json('country.json')

country[['Continent']].distinct().show()