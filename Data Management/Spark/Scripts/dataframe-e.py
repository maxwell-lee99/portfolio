from pyspark.sql import SparkSession
import pyspark.sql.functions as fc

spark = SparkSession.builder.appName("HW5").getOrCreate()

country=spark.read.json('country.json')

country.groupBy('Continent').agg(fc.avg('LifeExpectancy').alias("avg_le"),\
fc.count("*").alias("cnt")).filter('cnt >= 20').orderBy(fc.desc('cnt')).limit(1).show()