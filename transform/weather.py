from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType, TimestampType
import pyspark.sql.functions as F
 

# Define UDFs for converting latitude and longitude
def convert_latitude(x):
    direction = str(x)[-1]
    if direction == 'N':
        return float(str(x)[:-1])
    else:
        return -float(str(x)[:-1])
udf_convert_latitude = udf(lambda x: convert_latitude(x), FloatType())

def convert_longitude(x):
    direction = str(x)[-1]
    if direction == 'E':
        return float(str(x)[:-1])
    else:
        return -float(str(x)[:-1])
udf_convert_longitude = udf(lambda x: convert_longitude(x), FloatType())

# Set threshold date for filtering
thres = F.to_date(F.lit("2013-08-01")).cast(TimestampType())

# Read and transform weather data
us_wea = spark.read.format('csv').option("header", "true").load('s3://kai-airflow-storage/p2data/world_temperature/GlobalLandTemperaturesByCity.csv', inferSchema=True)\
                                 .where(col("dt") > thres)\
                                 .withColumn("latitude", udf_convert_latitude("Latitude"))\
                                 .withColumn("longitude", udf_convert_longitude("Longitude"))\
                                 .withColumnRenamed("AverageTemperature", "avg_temp")\
                                 .withColumnRenamed("AverageTemperatureUncertainty", "std_temp")\
                                 .withColumnRenamed("City", "city")\
                                 .withColumnRenamed("Country", "country")\
                                 .where(col("avg_temp").isNotNull())\
                                 .filter("country = 'United States'")\
                                 .drop("dt", "country", "Latitude", "Longitude")

# Read city data
city = spark.read.parquet("s3://kai-airflow-storage/lake/city/")

# Join with city data and write the result
us_wea = us_wea.join(city, "city", "left")\
               .drop("city", "state_code", "city_latitude", "city_longitude")
us_wea.write.mode("overwrite").parquet("s3://kai-airflow-storage/lake/us_cities_temperatures/")
