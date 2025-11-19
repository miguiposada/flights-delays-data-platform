from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    IntegerType, 
    TimestampType,
    DecimalType,
    DoubleType,
    DateType,
    LongType
)


#Vamos a establecer el esquema
silver_flightDelays_schema = StructType([
    StructField("ingestion_date", DateType(), True),
    StructField("Year", LongType(), True),
    StructField("Month", LongType(), True),
    StructField("DayofMonth", LongType(), True),
    StructField("FlightDate", DateType(), True),

    StructField("Marketing_Airline_Network", StringType(), True),
    StructField("OriginCityName", StringType(), True),
    StructField("DestCityName", StringType(), True),

    StructField("CRSDepTime", LongType(), True),
    StructField("DepTime", LongType(), True),
    StructField("DepDelay", DoubleType(), True),
    StructField("DepDelayMinutes", DoubleType(), True),

    StructField("TaxiOut", DoubleType(), True),
    StructField("WheelsOff", LongType(), True),
    StructField("WheelsOn", LongType(), True),
    StructField("TaxiIn", DoubleType(), True),

    StructField("CRSArrTime", LongType(), True),
    StructField("ArrTime", LongType(), True),
    StructField("ArrDelay", DoubleType(), True),
    StructField("ArrDelayMinutes", DoubleType(), True),

    StructField("CRSElapsedTime", DoubleType(), True),
    StructField("ActualElapsedTime", DoubleType(), True),
    StructField("AirTime", DoubleType(), True),
    StructField("Distance", DoubleType(), True),
    StructField("DistanceGroup", LongType(), True),

    StructField("CarrierDelay", DoubleType(), True),
    StructField("WeatherDelay", DoubleType(), True),
    StructField("NASDelay", DoubleType(), True),
    StructField("SecurityDelay", DoubleType(), True),
    StructField("LateAircraftDelay", DoubleType(), True),

    StructField("__index_level_0__", LongType(), True)   # índice auxiliar (si proviene de pandas)
])


#Vamos a establecer el esquema
gold_flightDelays_input_schema = StructType([
    StructField("ingestion_date", DateType(), True),
    StructField("Year", LongType(), True),
    StructField("Month", LongType(), True),
    StructField("DayofMonth", LongType(), True),
    StructField("FlightDate", DateType(), True),

    StructField("Marketing_Airline_Network", StringType(), True),
    StructField("OriginCityName", StringType(), True),
    StructField("DestCityName", StringType(), True),

    StructField("CRSDepTime", LongType(), True),
    StructField("DepTime", LongType(), True),
    StructField("DepDelay", DoubleType(), True),
    StructField("DepDelayMinutes", DoubleType(), True),

    StructField("TaxiOut", DoubleType(), True),
    StructField("WheelsOff", LongType(), True),
    StructField("WheelsOn", LongType(), True),
    StructField("TaxiIn", DoubleType(), True),

    StructField("CRSArrTime", LongType(), True),
    StructField("ArrTime", LongType(), True),
    StructField("ArrDelay", DoubleType(), True),
    StructField("ArrDelayMinutes", DoubleType(), True),

    StructField("CRSElapsedTime", DoubleType(), True),
    StructField("ActualElapsedTime", DoubleType(), True),
    StructField("AirTime", DoubleType(), True),
    StructField("Distance", DoubleType(), True),
    StructField("DistanceGroup", LongType(), True),

    StructField("CarrierDelay", DoubleType(), True),
    StructField("WeatherDelay", DoubleType(), True),
    StructField("NASDelay", DoubleType(), True),
    StructField("SecurityDelay", DoubleType(), True),
    StructField("LateAircraftDelay", DoubleType(), True),

    StructField("__index_level_0__", LongType(), True)   # índice auxiliar (si proviene de pandas)
])