# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
 

# Read data from new S3 paths
us_airport = spark.read.format('csv').option("header", "true").load('s3://kai-airflow-storage/p2data/airport_codes/airport-codes.csv', inferSchema=True)
demo = spark.read.format('csv').option("header", "true").load('s3://kai-airflow-storage/p2data/us_city_demographics/us-cities-demographics.csv', sep=';', inferSchema=True)

# Transformations (assuming the transformations are correct, focusing on paths)
city = us_airport.selectExpr("municipality AS city").union(demo.select("City as city")).distinct().withColumn("city_id", monotonically_increasing_id())

# Write result to new S3 path
city.write.mode("overwrite").parquet("s3://kai-airflow-storage/lake/city/")
