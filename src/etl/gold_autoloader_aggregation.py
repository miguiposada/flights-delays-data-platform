#Importamos las librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

from utils.read_json_from_blob import readJsonFromBlobWithSas
from utils.create_sas_conection import get_sas_details, configure_sas_access

import logging
# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
import json

from datetime import datetime
logging.info("-Se han importado las librerias")
import sys
print(sys.argv)


def gold_autoloader_aggregation(dataset_sas_details, inputConfiguration, outputConfiguration):
    try:
        logging.info("--> gold_autoloader_aggregation: Comienza el metodo bronze_ingestion")
        
        spark = SparkSession.builder.appName("gold_autoloader_aggregation").getOrCreate()
        logging.info("- Se ha creado la sesion de spark")

        configure_sas_access(spark, dataset_sas_details) 
        logging.info(f"Se ha configurado la conexion SAS")
        
        inputFormat=inputConfiguration['format'] if 'format' in inputConfiguration else "cloudFiles"
        datasetInputPath=inputConfiguration['datasetInputPath']
        autoloaderOptions=inputConfiguration['autoloaderOptions']
        
        
        logging.info(f"Ruta de origen para Auto Loader: {datasetInputPath}")

        # 3. Leer los datos
        logging.info(f"Se va a proceder a leer/creado el dataset de entrada")
        df_input = (
            spark.readStream
            .format(inputFormat)
            .options(**autoloaderOptions)
            .load(datasetInputPath)
        )
        logging.info(f"DataFrame de entrada df_input creado.")
        logging.info(f"El esquema del dataset de entrada ({datasetInputPath}) es el siguiente: ")
        df_input.printSchema()


        
        #Agregaciones
        df_input=df.withWatermark("eventTime", "10 minutes")
        df = df_input.groupBy("Year", "Month", "DayofMonth","OriginCityName","DestCityName").agg(
            count("*").alias("flights_count"),
            avg("DepDelayMinutes").alias("avg_dep_delay_minutes"),
            sum(when(col("ArrDelayMinutes") > 15, 1).otherwise(0)).alias("% vuelos con delay >15min"),
            avg("TaxiOut").alias("avg_taxi_out"),
            avg("TaxiIn").alias("avg_taxi_in")
        )
        

        logging.info(f"Se va a proceder a lanzar el proceso de streaming")
        outputFormat=outputConfiguration['format'] if 'format' in outputConfiguration else "cloudFiles"
        datasetOuputPath=outputConfiguration['datasetOuputPath']
        checkpointPath=outputConfiguration['checkpointPath']
        outputMode=outputConfiguration['outputMode'] if 'outputMode' in outputConfiguration else "append"


        if 'partitionBy' in outputConfiguration:
            df_output=(df.writeStream
                .format(outputFormat) # ⬅️ Formato de escritura ajustado a PARQUET
                .option("path", datasetOuputPath) # Especificar la ruta de destino
                .option("checkpointLocation", checkpointPath) 
                .partitionBy(outputConfiguration['partitionBy'])
                .outputMode(outputMode)                            
                .trigger(availableNow=True)                      
                .start() # Usamos .start() para iniciar el streaming
            )

        else:

            df_output=(df.writeStream
                .format(outputFormat) # ⬅️ Formato de escritura ajustado a PARQUET
                .option("path", datasetOuputPath) # Especificar la ruta de destino
                .option("checkpointLocation", checkpointPath) 
                .outputMode(outputMode)                            
                .trigger(availableNow=True)                      
                .start() # Usamos .start() para iniciar el streaming
            )

        logging.info(f"El proceso de streaming ha sido iniciado.")
        
        df_output.awaitTermination()
        logging.info(f"El dataset se ha leido correctamente y el stream ha finalizado (availableNow=True).")


        #--------------------------------------------------------------------------------------------------------------
        """         
        spark = SparkSession.builder.appName("ExtraccionDatagold_aggregationbricks").getOrCreate()
        ingestion_date = datetime.now().strftime("%Y_%m_%d")
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
        dataset_input_path = dataset_input_path.replace('YYYY_MM_DD', ingestion_date)
        
        input_path=f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/{dataset_input_path}"

        df_input=spark.read.format("parquet") \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .load(input_path)
        
        logging.info(f"El dataset de entrada tiene: {df_input.count()} filas")

        df_input.show(5)        # Muestra las primeras 5 filas
        df_input.printSchema()  # Muestra el esquema del DataFrame

        logging.info("- Se ha leido el dataset correspondiente")
        



        #Agregaciones

        df_output = df_input.groupBy("Year", "Month", "DayofMonth","OriginCityName","DestCityName").agg(
            count("*").alias("flights_count"),
            avg("DepDelayMinutes").alias("avg_dep_delay_minutes"),
            sum(when(col("ArrDelayMinutes") > 15, 1).otherwise(0)).alias("% vuelos con delay >15min"),
            avg("TaxiOut").alias("avg_taxi_out"),
            avg("TaxiIn").alias("avg_taxi_in")
        )

        df_output.show(10)  



        logging.info(f"El dataset de salida tiene: {df_output.count()} filas")

        logging.info("- Se va a proceder a guardar el archivo correspondiente en el ruta deseada")

        #Guardamos como parquet
        dataset_output_path = dataset_output_path.replace('YYYY_MM_DD', ingestion_date)
        output_path=f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/{dataset_output_path}"

        # Escribe el DataFrame en formato Parquet
        df_output.write.format("parquet") \
          .mode("overwrite") \
          .save(output_path)

        logging.info(f"- Se ha guardado con exito el archivo en  {output_path}")

        """
    except Exception as e:
        logging.error(f"Ocurrió un error al extraer los datos: {e}")
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
        config_sas_details = get_sas_details(storage_account_name,config_container, key_vault_name, sastoken_config_secret_name)#,configs_folder_path)
        logging.info(f"Los config_sas_details details son: {config_sas_details}")
        
        # 2. Recuperar el fichero de configuracion
        logging.info(f"2. Se va a proceder a recuperar el fichero de configuracion")
        configJSON = readJsonFromBlobWithSas(config_sas_details['storage_account'], config_sas_details['container_name'],config_blob_path,config_sas_details['sas_token'])
        logging.info(f"El fihcero de configuracion {config_blob_path} es: {configJSON}")

        # 3. Obtener detalles de la conexión Sas para acceder a los datasets
        logging.info(f"3. A partir de los datos del fichero de configuracion se va a proceder a recuperar la configuracion sas para acceder a los datasets")
        datasetSecurityConfiguration=configJSON['datasetSecurityConfiguration']
        dataset_sas_details = get_sas_details(datasetSecurityConfiguration['datasetStorageAccount'],datasetSecurityConfiguration['datasetContainer'],
                                               datasetSecurityConfiguration['datasetKeyVaultName'], datasetSecurityConfiguration['SasTokenSecretName'])#, datasetConfiguration['SasPath'])
        logging.info(f"Los dataset_sas_details details son: {dataset_sas_details}")

        # 4. Se  va a proceder con la ingestion de los datos
        logging.info(f"4. Se  va a proceder con la ingestion de los datos")
        
        gold_autoloader_aggregation(dataset_sas_details, configJSON['streamConfiguration']['inputConfiguration'],  configJSON['streamConfiguration']['outputConfiguration'])


    except Exception as e:
        logging.error(f"Ocurrió un error: {e}")


if __name__ == "__main__":
    main()