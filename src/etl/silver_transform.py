#Importamos las librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

from utils.read_json_from_blob import readJsonFromBlob
from utils.read_azure_secret import readAzureSecret

import logging
# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
import json
logging.info("-Se han importado las librerias")
import sys
print(sys.argv)






def silver_transform(storage_account_name,storage_account_access_key,dataset_container_name,dataset_input_path,dataset_output_path):
    try:

        spark = SparkSession.builder.appName("ExtraccionDatasilver_transform").getOrCreate()
        
        # --------------------------------------------
        # CONFIGURACIÓN DE ACCESO A BLOB STORAGE
        # --------------------------------------------
        # Configurar Spark para acceder a Blob Storage
        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
            storage_account_access_key
        )

        logging.info("- Se han establecido los paths para la lectura de los archivos")

        #Leemos df
        input_path=f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/{dataset_input_path}"

        df_input=spark.read.format("parquet") \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .load(input_path)

        df_input.show(5)        # Muestra las primeras 5 filas
        df_input.printSchema()  # Muestra el esquema del DataFrame

        logging.info("- Se ha leido el dataset correspondiente")



        #Vamos a establecer el esquema
        schema_df_input = StructType([
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
        #Aplicamos el schema
        for field in schema_df_input.fields:
            if field.name in df_input.columns:
                df_output = df_input.withColumn(field.name, col(field.name).cast(field.dataType))

        df_output.printSchema()

        #Vamos a agregar las columnas y eliminar datos incompletos
        #Eliminamos duplicados basandonos en las columnas clave
        df_output = df_output.dropDuplicates(["FlightDate", "Marketing_Airline_Network", "OriginCityName", "DestCityName", "CRSDepTime"])

        #Reemplazamos nulos por 0 en las columnas delay y los negativos tambien
        cols_delay = [
            "DepDelay",
            "DepDelayMinutes",
            "TaxiOut",
            "TaxiIn",
            "ArrDelay",
            "ArrDelayMinutes",
            "CRSElapsedTime",
            "ActualElapsedTime",
            "AirTime",
            "Distance",
            "CarrierDelay",
            "WeatherDelay",
            "NASDelay",
            "SecurityDelay",
            "LateAircraftDelay"
        ]
        # Reemplazar nulos
        df_output = df_output.na.fill(0, cols_delay)
        # Reemplazar negativos
        for c in cols_delay:
            df_output = df_output.withColumn(c, when(col(c) < 0, 0).otherwise(col(c)))

        #Crear columna flag is_delayed_15
        df_output = df_output.withColumn(
            "is_delayed_15",
            when(col("ArrDelayMinutes") >= 15, 1).otherwise(0)
        )
        logging.info(f"El numero total de filas antes de la limpieza es: {df_input.count()} ")
        logging.info(f"El numero total de filas despues de la limpieza es: {df_output.count()}")



        logging.info("- Se va a proceder a guardar el archivo correspondiente en el ruta deseada")

        #Guardamos como parquet
        output_path=f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/{dataset_output_path}"

        # Escribe el DataFrame en formato Parquet
        df_output.write.format("parquet") \
          .mode("overwrite") \
          .save(output_path)
        logging.info(f"- Se ha guardado con exito el archivo en  {output_path}")

    except Exception as e:
        logging.error(f"Ocurrió un error al extraer los datos: {e}")
    finally:
        logging.info("Proceso de extracción finalizado")













def main():
    try:
        logging.info("- El cluster ha arrancado y el proceso va a iniciar")
        #key_vault_name = "databrickslearningkvmp"
        #secret_name = "databrickslearningsecretmp-accesskey"
        key_vault_name = sys.argv[1]
        secret_name = sys.argv[2]
        logging.info(f"El key_vault_name es: {key_vault_name} y el secret_name es: '{secret_name}'")

        
        storage_account_access_key=readAzureSecret(key_vault_name, secret_name)
        logging.info(f"El secreto es: '{storage_account_access_key}'")

        storage_account_name = sys.argv[3]
        config_container = sys.argv[4]
        config_blob_path = sys.argv[5]        

        configJSON = readJsonFromBlob(storage_account_name, "databricks-projects","Flight_Delays/config/bronze_ingestion_config.json",storage_account_access_key)
        logging.info(f"El configJSON es: {configJSON}")


        silver_transform(storage_account_name, storage_account_access_key,
                        configJSON['dataset_container_name'],configJSON['dataset_input_path'],configJSON['dataset_output_path'])

    except Exception as e:
        logging.error(f"Ocurrió un error: {e}")


if __name__ == "__main__":
    main()