
#Importamos las librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
def read_azure_secret(key_vault_name: str, secret_name: str) -> str:
    """
    Lee un secreto desde Azure Key Vault.

    :param key_vault_name: Nombre del Key Vault (sin https://) y debe ser de tipo string
    :param secret_name: Nombre del secreto que quieres obtener y debe ser de tipo string
    :return: Valor del secreto de tipos tring
    """
    try:
        logging.info("- Se va a proceder a recuperar el secreto")

        key_vault_url = f"https://{key_vault_name}.vault.azure.net/"
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=key_vault_url, credential=credential)

        logging.info(f"Obteniendo secreto '{secret_name}' desde Key Vault '{key_vault_name}'")
        secret = client.get_secret(secret_name)
        logging.info("- Se ha recuperado el secreto con exito")
        return secret.value

    except Exception as e:
        logging.error(f"No se pudo obtener el secreto: {e}")
        raise


def bronze_ingestion(storage_account_name,storage_account_access_key):
    try:

        spark = SparkSession.builder.appName("ExtraccionDatabronze_ingestionbricks").getOrCreate()
        
        # --------------------------------------------
        # CONFIGURACIÓN DE ACCESO A BLOB STORAGE
        # --------------------------------------------
        container_name = "databricks-projects"


        # Configurar Spark para acceder a Blob Storage
        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
            storage_account_access_key
        )

        logging.info("- Se han establecido los paths para la lectura de los archivos")

        #Leemos df
        relative_input_folder_path="Flight_Delays/data/raw/Flight_Delay.parquet"
        raw_input_path=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{relative_input_folder_path}"

        df_raw=spark.read.format("parquet") \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .load(raw_input_path)

        df_raw.show(5)        # Muestra las primeras 5 filas
        df_raw.printSchema()  # Muestra el esquema del DataFrame

        logging.info("- Se ha leido el dataset correspondiente")


        # Agregar columnas de metadatos: fecha de ingesta, nombre del archivo, etc.
        #Añadimos columna current timestamp
        df_output=df_raw.select(
            current_date().alias("ingestion_date"),  # primera columna
            *df_raw.columns                    # resto de columnas
        )
        df_output.show(5)        # Muestra las primeras 5 filas
        df_output.printSchema()

        logging.info("- Se ha añadido la columna ingestion_date al dataset")

        logging.info("- Se va a proceder a guardar el archivo correspondiente en el ruta deseada")

        #Guardamos como parquet
        relative_output_folder_path="Flight_Delays/data/bronze/Flight_Delay_bronze.parquet"
        output_path=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{relative_output_folder_path}"

        # Escribe el DataFrame en formato Parquet
        df_output.write.format("parquet") \
          .mode("overwrite") \
          .save(output_path)

    except Exception as e:
        logging.error(f"Ocurrió un error al extraer los datos: {e}")
    finally:
        logging.info("Proceso de extracción finalizado")



def main():
    try:
        
        logging.info("- El cluster ha arrancado y el proceso va a iniciar")
        key_vault_name = "databrickslearningkvmp "
        secret_name = "databrickslearningsecretmp-accesskey"

        storage_account_name = "databrickslearningsamp"
        storage_account_access_key=read_azure_secret(key_vault_name, secret_name)

        logging.info(f"El secreto es: '{storage_account_access_key}'")
        
        bronze_ingestion(storage_account_name, storage_account_access_key)
    except Exception as e:
        logging.error(f"Ocurrió un error: {e}")


if __name__ == "__main__":
    main()