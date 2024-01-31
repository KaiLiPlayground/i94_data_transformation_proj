from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import LongType 

# Read city data from the lake
city = spark.read.parquet("s3://kai-airflow-storage/lake/city/")

# Read and transform demographics data
demo = spark.read.format('csv').option("header", "true").load('s3://kai-airflow-storage/p2data/us_city_demographics/us-cities-demographics.csv', sep=';', inferSchema=True)\
                .withColumn("male_population", col("Male Population").cast(LongType()))\
                .withColumn("female_population", col("Female Population").cast(LongType()))\
                .withColumn("total_population", col("Total Population").cast(LongType()))\
                .withColumn("num_veterans", col("Number of Veterans").cast(LongType()))\
                .withColumn("foreign_born", col("Foreign-born").cast(LongType()))\
                .withColumnRenamed("Average Household Size", "avg_household_size")\
                .withColumnRenamed("State Code", "state_code")\
                .withColumnRenamed("Race","race")\
                .withColumnRenamed("Median Age", "median_age")\
                .withColumnRenamed("City", "city")\
                .drop("State", "Count", "Male Population", "Female Population", "Total Population", "Number of Veterans", "Foreign-born")

# Join with city data and write the result
demo = demo.join(city, (demo.city == city.city) & (demo.state_code == city.state_code), "left")\
           .drop("city", "state_code")
demo.write.mode("overwrite").parquet("s3://kai-airflow-storage/lake/us_cities_demographics/")
