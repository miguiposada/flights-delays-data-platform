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


def gold_aggregation(storage_account_name,storage_account_access_key,dataset_container_name,dataset_input_path,dataset_output_path):
    try:

        spark = SparkSession.builder.appName("ExtraccionDatagold_aggregationbricks").getOrCreate()
        
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
        display(df_input.limit(10))



        #Agregaciones

        df_output = df_input.groupBy("Year", "Month", "DayofMonth","OriginCityName","DestCityName").agg(
            count("*").alias("flights_count"),
            avg("DepDelayMinutes").alias("avg_dep_delay_minutes"),
            sum(when(col("ArrDelayMinutes") > 15, 1).otherwise(0)).alias("% vuelos con delay >15min"),
            avg("TaxiOut").alias("avg_taxi_out"),
            avg("TaxiIn").alias("avg_taxi_in")
        )

        display(df_output.limit(10))




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
        
        configJSON = readJsonFromBlob(storage_account_name, config_container,config_blob_path,storage_account_access_key)
        logging.info(f"El configJSON es: {configJSON}")

        gold_aggregation(storage_account_name, storage_account_access_key,
                        configJSON['dataset_container_name'],configJSON['dataset_input_path'],configJSON['dataset_output_path'])

    except Exception as e:
        logging.error(f"Ocurrió un error: {e}")


if __name__ == "__main__":
    main()