from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
 

# Define UDFs for parsing latitude, longitude, and state
def parse_lat(x):
    y = x.strip().split(',')
    return float(y[0])
udf_parse_lat = udf(lambda x: parse_lat(x), FloatType())

def parse_long(x):
    y = x.strip().split(',')
    return float(y[1])
udf_parse_long = udf(lambda x: parse_long(x), FloatType())

def parse_state(x):
    return x.strip().split('-')[-1]
udf_parse_state = udf(lambda x: parse_state(x), StringType())

# Read city data from the lake
city = spark.read.parquet("s3://kai-airflow-storage/lake/city/")

# Read, transform, and join airport codes data
us_airport = spark.read.format('csv').option("header", "true").load('s3://kai-airflow-storage/p2data/airport_codes/airport-codes.csv', inferSchema=True)\
                        .withColumn("airport_latitude", udf_parse_lat("coordinates"))\
                        .withColumn("airport_longitude", udf_parse_long("coordinates"))\
                        .filter("iso_country = 'US'")\
                        .withColumn("state", udf_parse_state("iso_region"))\
                        .withColumnRenamed("ident", "icao_code")\
                        .drop("coordinates", "gps_code", "local_code", "continent", "iso_region", "iso_country")

us_airport = us_airport.join(city, (us_airport.municipality == city.city) & (us_airport.state == city.state_code), "left")\
                        .drop("municipality", "state", "city", "state_code")

# Write the transformed and joined data
us_airport.write.mode("overwrite").parquet("s3://kai-airflow-storage/lake/codes/airport_code/")
