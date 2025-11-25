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



def bronze_ingestion(dataset_sas_details, datasetInputPath, datasetOuputPath, checkpointPath, autoloaderOptions):
    try:
        logging.info("- bronze_ingestion --> Comienza el metodo bronze_ingestion")
        
        spark = SparkSession.builder.appName("ExtraccionDatabronze_ingestionbricks").getOrCreate()
        logging.info("- Se ha creado la sesion de spark")

        configure_sas_access(spark, dataset_sas_details) 
        logging.info(f"Se ha configurado la conexion SAS")
        
        
        
        
        
        
        #PATHS Y VARIABLES

        #CHECKPOINT_LOCATION = "/mnt/datalake/autoloader_checkpoints/ventas_incremental_parquet" # Checkpoint location
        #CHECKPOINT_LOCATION = "wasbs://databricks-projects@databrickslearningsamp.blob.core.windows.net/Flight_Delays/data/checkpoint/" # Checkpoint location
        #TARGET_OUTPUT_PATH = "wasbs://databricks-projects@databrickslearningsamp.blob.core.windows.net/Flight_Delays/data/bronze_autoloader/"

        #input_path = dataset_sas_details["source_path"] #+ "raw/tests/" 
        # Asume que tus ficheros de datos están en /data/raw/ventas/ dentro del blob

        logging.info(f"Ruta de origen para Auto Loader: {datasetInputPath}")

        """
        # 2. Configuración de Auto Loader para leer Parquet
        autoloader_options = {
            "cloudFiles.format": "parquet",#"**parquet**", # ⬅️ Formato de lectura ajustado a PARQUET
            "cloudFiles.schemaLocation": CHECKPOINT_LOCATION+"/schema/",
            "cloudFiles.maxFilesPerTrigger": "100",
            "cloudFiles.inferColumnTypes": "true",
            #"cloudFiles.allowCdcSchemaEvolution": "true",
            "cloudFiles.rescuedDataColumn": "_rescued_data" 
        }
        """

        # 3. Leer los datos
        logging.info(f"Se va a proceder a leer/creado el dataset de entrada")
        df_input = (
            spark.readStream
            .format("cloudFiles")
            .options(**autoloaderOptions)
            .load(datasetInputPath)
        )
        logging.info(f"DataFrame de entrada df_input creado.")
        logging.info(f"El esquema del dataset de entrada ({datasetInputPath}) es: {df_input.printSchema()} ")
                
        logging.info(f"Se va a proceder a crear el dataset de salida y realizar las operaciones en caso de que sea necesario")
        #Añadimos columna current timestamp
        df=df_input.select(
            current_date().alias("ingestion_date"), 
            *df_input.columns
        )


        logging.info(f"Se va a proceder a lanzar el proceso de streaming")
        df_output=(df.writeStream
            .format("parquet") # ⬅️ Formato de escritura ajustado a PARQUET
            .option("path", datasetOuputPath) # Especificar la ruta de destino
            .option("checkpointLocation", checkpointPath) 
            .outputMode("append")                            
            .trigger(availableNow=True)                      
            .start() # Usamos .start() para iniciar el streaming
        )
        logging.info(f"El proceso de streaming ha sido iniciado.")
        
        df_output.awaitTermination()
        logging.info(f"El dataset se ha leido correctamente y el stream ha finalizado (availableNow=True).")


        """ 
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
        """

    except Exception as e:
        logging.error(f"Ocurrió un error al extraer los datos: {e}")
        raise e
    finally:
        logging.info("Proceso de extracción finalizado")


def main():
    try:
        logging.info("- El cluster ha arrancado y el proceso va a iniciar")
        
        key_vault_name = sys.argv[1]
        sastoken_config_secret_name = sys.argv[2]

        storage_account_name = sys.argv[3]
        config_container = sys.argv[4]

        config_blob_path = sys.argv[5]
        configs_folder_path = sys.argv[6]
        
        # 0. Se han obtenido las siguientes variables a partir de los parametros de entrada:
        logging.info(f"0. Se han obtenido las siguientes variables a partir de los parametros de entrada: 'key_vault_name': '{key_vault_name}', 'sastoken_config_secret_name': '{sastoken_config_secret_name}', 'storage_account_name': '{storage_account_name}', 'config_container': '{config_container}','config_blob_path': '{config_blob_path}', 'configs_folder_path': '{configs_folder_path}'")

        
        # 1. Obtener detalles de la conexión Sas para recuperar el fichero de configuracion
        logging.info(f"1. Se va a proceder a recuperar la configuracion sas correspondiente al fichero de configuracion")
        logging.info(f"El key_vault_name es: {key_vault_name} y el secret_name es: '{sastoken_config_secret_name}'")
        config_sas_details = get_sas_details(storage_account_name,config_container, key_vault_name, sastoken_config_secret_name,configs_folder_path)
        logging.info(f"Los config_sas_details details son: {config_sas_details}")
        
        # 2. Recuperar el fichero de configuracion
        logging.info(f"2. Se va a proceder a recuperar el fichero de configuracion")
        configJSON = readJsonFromBlobWithSas(config_sas_details['storage_account'], config_sas_details['container_name'],config_blob_path,config_sas_details['sas_token'])
        logging.info(f"El fihcero de configuracion {config_blob_path} es: {configJSON}")

        # 3. Obtener detalles de la conexión Sas para acceder a los datasets
        logging.info(f"3. A partir de los datos del fichero de configuracion se va a proceder a recuperar la configuracion sas para acceder a los datasets")
        datasetConfiguration=configJSON['datasetConfiguration']
        dataset_sas_details = get_sas_details(datasetConfiguration['datasetStorageAccount'],datasetConfiguration['datasetContainer'],
                                               datasetConfiguration['datasetKeyVaultName'], datasetConfiguration['SasTokenSecretName'], datasetConfiguration['SasPath'])
        logging.info(f"Los dataset_sas_details details son: {dataset_sas_details}")

        # 4. Se  va a proceder con la ingestion de los datos
        logging.info(f"4. Se  va a proceder con la ingestion de los datos")
        bronze_ingestion(dataset_sas_details,  configJSON['datasetInputPath'],  configJSON['datasetOuputPath'], configJSON['checkpointPath'], configJSON['autoloaderOptions'])

        
        
    except Exception as e:
        logging.error(f"Ocurrió un error: {e}")


if __name__ == "__main__":
    main()