import logging 
spark.sparkContext.setLogLevel('WARN')

# Setup logging
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger("DAG")

def check(path, table):
    df = spark.read.parquet(path)
    if len(df.columns) > 0 and df.count() > 0:
        logger.warn(f"{table} SUCCESS")
    else:
        logger.warn(f"{table} FAIL")

# Perform checks
check("s3://kai-airflow-storage/lake/city/", "city")
check("s3://kai-airflow-storage/lake/codes/state_code/", "state_code")
check("s3://kai-airflow-storage/lake/codes/country_code/", "country_code")
check("s3://kai-airflow-storage/lake/codes/airport_code/", "airport_code")
check("s3://kai-airflow-storage/lake/us_cities_demographics/", "us_cities_demographics")
check("s3://kai-airflow-storage/lake/us_cities_temperatures/", "us_cities_temperatures")
check("s3://kai-airflow-storage/lake/us_airports_weather/", "us_airport_weather")
