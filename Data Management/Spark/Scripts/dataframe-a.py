from pyspark.sql import SparkSession
import pyspark.sql.functions as fc

spark = SparkSession.builder.appName("HW5").getOrCreate()

country=spark.read.json('country.json')

# country[['Name']].filter("Continent='North America'").show()

country.groupBy('Continent').agg(fc.count('*')).show()