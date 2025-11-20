import sys

from datetime import datetime

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

logging.info("-Se han importado las librerias")

print(sys.argv)



def bronze_ingestion(storage_account_name,storage_account_access_key,dataset_container_name,dataset_input_path,dataset_output_path):
    try:

        spark = SparkSession.builder.appName("ExtraccionDatabronze_ingestionbricks").getOrCreate()
        logging.info("- Se ha creado la sesion de spark")
        # --------------------------------------------
        # CONFIGURACIÓN DE ACCESO A BLOB STORAGE
        # --------------------------------------------
        # Configurar Spark para acceder a Blob Storage
        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
            storage_account_access_key
        )

        
        #Leemos df
        raw_input_path=f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/{dataset_input_path}"
        logging.info(f"- Se va a proceder a leer el archivo de entrada del path {raw_input_path}")
        
        checkpoint_path  = "dbfs:/mnt/bronze/checkpoints/flights/"
        df_raw=(spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("cloudFiles.schemaLocation", checkpoint_path + "schema/")
            .load(f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/Flight_Delays/data/bronze")
        )
        logging.info(f"El dataset de entrada tiene: {df_raw.count()} filas")

        df_raw.show(5)        # Muestra las primeras 5 filas
        df_raw.printSchema()  # Muestra el esquema del DataFrame

        logging.info(f"- Se ha leido el dataset {raw_input_path} ")


        # Agregar columnas de metadatos: fecha de ingesta, nombre del archivo, etc.
        #Añadimos columna current timestamp
        df_output=df_raw.select(
            current_date().alias("ingestion_date"),  # primera columna
            *df_raw.columns                    # resto de columnas
        )
        df_output.show(5)        # Muestra las primeras 5 filas
        df_output.printSchema()

        logging.info("- Se ha añadido la columna ingestion_date al dataset")
        logging.info(f"El dataset de salida tiene: {df_output.count()} filas")
        
        ingestion_date = datetime.now().strftime("%Y_%m_%d")
        dataset_output_path = dataset_output_path.replace('YYYY_MM_DD', ingestion_date)
        logging.info(f"- Se va a proceder a guardar el archivo correspondiente en el ruta {dataset_output_path}")
        
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

        bronze_ingestion(storage_account_name, storage_account_access_key,
                        configJSON['dataset_container_name'],configJSON['dataset_input_path'],configJSON['dataset_output_path'])

    except Exception as e:
        logging.error(f"Ocurrió un error: {e}")


if __name__ == "__main__":
    main()