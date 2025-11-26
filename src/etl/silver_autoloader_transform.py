#Importamos las librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

from utils.read_json_from_blob import readJsonFromBlobWithSas
from utils.create_sas_conection import get_sas_details, configure_sas_access
from utils.schemas import silver_flightDelays_schema


import logging
# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from datetime import datetime
logging.info("-Se han importado las librerias")
import sys
print(sys.argv)






def silver_autoloader_transform(dataset_sas_details, datasetInputPath, datasetOuputPath, checkpointPath, autoloaderOptions):
    try:
        logging.info("--> silver_autoloader_transform: Comienza el metodo bronze_ingestion")
        
        spark = SparkSession.builder.appName("ExtraccionDatasilver_transform").getOrCreate()
        logging.info("- Se ha creado la sesion de spark")

        configure_sas_access(spark, dataset_sas_details) 
        logging.info(f"Se ha configurado la conexion SAS")
        
        logging.info(f"Ruta de origen para Auto Loader: {datasetInputPath}")

        # 3. Leer los datos
        logging.info(f"Se va a proceder a leer/creado el dataset de entrada")
        df_input = (
            spark.readStream
            .format("cloudFiles")
            .options(**autoloaderOptions)
            .load(datasetInputPath)
        )
        logging.info(f"DataFrame de entrada df_input creado.")
        logging.info(f"El esquema del dataset de entrada ({datasetInputPath}) es el siguiente: ")
        df_input.printSchema()

        #Eliminamos duplicados basandonos en las columnas clave
        df = df_input.dropDuplicates(["FlightDate", "Marketing_Airline_Network", "OriginCityName", "DestCityName", "CRSDepTime"])
        
        logging.info(f"Se va a proceder a lanzar el proceso de streaming")
        df_output=(df.writeStream
            .format("parquet") # ⬅️ Formato de escritura ajustado a PARQUET
            .option("path", datasetOuputPath) # Especificar la ruta de destino
            .option("checkpointLocation", checkpointPath) 
            #.partitionBy("ingestion_date")
            .outputMode("append")                            
            .trigger(availableNow=True)                      
            .start() # Usamos .start() para iniciar el streaming
        )
        logging.info(f"El proceso de streaming ha sido iniciado.")
        
        df_output.awaitTermination()
        logging.info(f"El dataset se ha leido correctamente y el stream ha finalizado (availableNow=True).")

        
        """  

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



        
        #Guardamos como parquet
        dataset_output_path = dataset_output_path.replace('YYYY_MM_DD', ingestion_date)

        output_path=f"wasbs://{dataset_container_name}@{storage_account_name}.blob.core.windows.net/{dataset_output_path}"
        logging.info(f"- Se va a proceder a guardar el archivo correspondiente en : {output_path}")

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
        datasetConfiguration=configJSON['datasetConfiguration']
        dataset_sas_details = get_sas_details(datasetConfiguration['datasetStorageAccount'],datasetConfiguration['datasetContainer'],
                                               datasetConfiguration['datasetKeyVaultName'], datasetConfiguration['SasTokenSecretName'])#, datasetConfiguration['SasPath'])
        logging.info(f"Los dataset_sas_details details son: {dataset_sas_details}")

        # 4. Se  va a proceder con la ingestion de los datos
        logging.info(f"4. Se  va a proceder con la ingestion de los datos")


        #silver_autoloader_transform(storage_account_name, storage_account_access_key,
        #                configJSON['dataset_container_name'],configJSON['dataset_input_path'],configJSON['dataset_output_path'])
        silver_autoloader_transform(dataset_sas_details, datasetConfiguration['datasetInputPath'],  datasetConfiguration['datasetOuputPath'], datasetConfiguration['checkpointPath'], configJSON['autoloaderOptions'])

    except Exception as e:
        logging.error(f"Ocurrió un error: {e}")


if __name__ == "__main__":
    main()