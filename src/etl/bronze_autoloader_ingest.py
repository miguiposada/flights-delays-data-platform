import sys

from datetime import datetime

#Importamos las librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

from utils.read_json_from_blob import readJsonFromBlobWithSas
from utils.read_azure_secret import readAzureSecret
from utils.create_sas_conection import get_sas_details, configure_sas_access

import logging
# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info("-Se han importado las librerias")



def bronze_ingestion(storage_account_name,sas_details,dataset_container_name,dataset_input_path,dataset_output_path):
    try:

        #PATHS Y VARIABLES

        CHECKPOINT_LOCATION = "/mnt/datalake/autoloader_checkpoints/ventas_incremental_parquet" # Checkpoint location
        TARGET_OUTPUT_PATH = "wasbs://processeddata@tuaccountdlgen2.blob.core.windows.net/ventas_limpias_parquet/"


        spark = SparkSession.builder.appName("ExtraccionDatabronze_ingestionbricks").getOrCreate()
        logging.info("- Se ha creado la sesion de spark")
        # --------------------------------------------
        # CONFIGURACIÓN DE ACCESO A BLOB STORAGE
        # --------------------------------------------
        """ # 1. Obtener detalles de la conexión y configurar la SAS
        sas_details = get_sas_details(
            storage_account=storage_account_name,
            container_name=dataset_container_name,
            # sas_token_secret_scope="databricks-scope" # Descomenta si usas secrets
        )"""
        
        # 🚨 Es fundamental ejecutar la configuración de Spark ANTES de leer
        configure_sas_access(spark, sas_details) 

        input_path = sas_details["source_path"] + "ventas/" 
        # Asume que tus ficheros de datos están en /data/raw/ventas/ dentro del blob

        print(f"Ruta de origen para Auto Loader: {input_path}")


        # 2. Configuración de Auto Loader para leer Parquet
        autoloader_options = {
            "cloudFiles.format": "**parquet**", # ⬅️ Formato de lectura ajustado a PARQUET
            "cloudFiles.schemaLocation": CHECKPOINT_LOCATION,
            "cloudFiles.maxFilesPerTrigger": "100",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.allowCdcSchemaEvolution": "true",
            "cloudFiles.rescuedDataColumn": "_rescued_data" 
        }

        # 3. Leer los datos de forma incremental
        df_input = (
            spark.readStream
            .format("cloudFiles")
            .options(**autoloader_options)
            .load(input_path)
        )

        # 4. Aplicar transformaciones básicas (Opcional)
         # Agregar columnas de metadatos: fecha de ingesta, nombre del archivo, etc.
        #Añadimos columna current timestamp
        df_output=df_input.select(
            current_date().alias("ingestion_date"),  # primera columna
            *df_input.columns                    # resto de columnas
        )
        df_output.show(5)        # Muestra las primeras 5 filas
        df_output.printSchema()

        logging.info("- Se ha añadido la columna ingestion_date al dataset")
        logging.info(f"El dataset de salida tiene: {df_output.count()} filas")
        
        ingestion_date = datetime.now().strftime("%Y_%m_%d")
        dataset_output_path = dataset_output_path.replace('YYYY_MM_DD', ingestion_date)
        logging.info(f"- Se va a proceder a guardar el archivo correspondiente en el ruta {dataset_output_path}")
        

        # 5. Escribir los datos en formato Parquet en la ubicación de destino
        print(f"Escribiendo en la ruta de destino (Parquet): {TARGET_OUTPUT_PATH}")

        (
            df_output.writeStream
            .format("**parquet**") # ⬅️ Formato de escritura ajustado a PARQUET
            .option("path", TARGET_OUTPUT_PATH) # Especificar la ruta de destino
            .option("checkpointLocation", CHECKPOINT_LOCATION) 
            .outputMode("append")                            
            .trigger(availableNow=True)                      
            .start() # Usamos .start() para iniciar el streaming
        ).awaitTermination() # Espera a que el proceso termine (ya que usamos availableNow=True)

        print("Extracción incremental con Auto Loader finalizada y escrita en formato Parquet.")

        ##-----------------------------------------------------------------------------------------------------------
        #Leemos df
        #raw_input_path=f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/{dataset_input_path}"
        raw_input_path=f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/databricks-projects/Flight_Delays/data/raw/autoloader"
        output_path=f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/databricks-projects/Flight_Delays/data/bronze/autoloader"
        
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
        #output_path=f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/{dataset_output_path}"

        # Escribe el DataFrame en formato Parquet
        (df_output.writeStream
            .format("parquet")              # Formato de salida (e.g., parquet, delta, console)
            .option("checkpointLocation", checkpoint_path) # RUTA OBLIGATORIA para mantener el estado
            .outputMode("append")           # Modo de salida (append, complete, update)
            .start(f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/Flight_Delays/data/test")
        )
        """
          .write.format("parquet") \
          .mode("overwrite") \
          .save(output_path) 
        """
        
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
        config_secret_name = sys.argv[2]
        storage_account_name = sys.argv[3]
        config_container = sys.argv[4]
        config_blob_path = sys.argv[5]
        sastoken_config_secret_name = sys.argv[6]
        configs_folder_path = sys.argv[7]
        sastoken_bronzeconfig_secret_name = sys.argv[8]
        sastoken_datasets_secret_name = sys.argv[9]


        logging.info(f"El key_vault_name es: {key_vault_name} y el secret_name es: '{sastoken_config_secret_name}'")
        
        # 1. Obtener detalles de la conexión y configurar la SAS
        config_sas_details = get_sas_details(storage_account_name,config_container, key_vault_name, sastoken_config_secret_name,configs_folder_path)
        logging.info(f"Los config_sas_details details son: {config_sas_details}")
        
      
        configJSON = readJsonFromBlobWithSas(config_sas_details['storage_account'], config_sas_details['container_name'],config_blob_path,config_sas_details['sas_token'])
        
        logging.info(f"El configJSON {config_blob_path} es: {configJSON}")
        datasetConfiguration=configJSON['datasetConfiguration']
        dataset_sas_details = get_sas_details(datasetConfiguration['datasetStorageAccount'],datasetConfiguration['datasetContainerName'],
                                               datasetConfiguration['datasetKeyVaultName'], datasetConfiguration['SasTokenSecretName'], datasetConfiguration['SasPath'])
        logging.info(f"Los dataset_sas_details details son: {dataset_sas_details}")

        bronze_ingestion(storage_account_name, dataset_sas_details,
                        datasetConfiguration['datasetContainerName'],datasetConfiguration['datasetInputPath'],datasetConfiguration['datasetOuputPath'])

        
        
    except Exception as e:
        logging.error(f"Ocurrió un error: {e}")


if __name__ == "__main__":
    main()