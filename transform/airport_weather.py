# Read data from the updated paths
us_airport = spark.read.parquet("s3://kai-airflow-storage/lake/codes/airport_code/")
city = spark.read.parquet("s3://kai-airflow-storage/lake/city/")
state_code = spark.read.parquet("s3://kai-airflow-storage/lake/codes/state_code/")
us_wea = spark.read.parquet("s3://kai-airflow-storage/lake/us_cities_temperatures/")

# Perform transformations and joins
airport_weather = us_airport.select("name", "elevation_ft", "city_id")\
                            .join(city.select("city", "city_id", "state_code"), "city_id", "left")

airport_weather = airport_weather.join(state_code, airport_weather.state_code == state_code.code, "left")\
                                 .drop("state_code", "code")

airport_weather = airport_weather.join(us_wea, "city_id", "inner").drop("city_id")

# Write the transformed data
airport_weather.write.mode("overwrite").parquet("s3://kai-airflow-storage/lake/us_airports_weather/")
