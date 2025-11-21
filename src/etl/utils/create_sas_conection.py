import os

from utils.read_azure_secret import readAzureSecret

# --- Parámetros de conexión ---
# Nota: En un entorno de producción, la SAS NUNCA debe estar en el código.
# Utiliza Azure Key Vault o Databricks Secrets para almacenar la SAS de forma segura.

def get_sas_details(storage_account, container_name,key_vault_name, secret_name, path):
    """
    Retorna los parámetros de la cuenta de almacenamiento y la clave SAS.

    :param storage_account: Nombre de la cuenta de almacenamiento.
    :param container_name: Nombre del contenedor (blob) donde están los datos.
    :param sas_token_secret_scope: (Opcional) Scope de Databricks Secrets si se usa.
    :return: dict con los parámetros de conexión.
    """
    
    # 🚨 **Reemplaza esto con tu valor real de SAS** 🚨
    # Mejor práctica: Usar dbutils.secrets.get()
    if os.environ.get("ADLS_SAS_TOKEN"):
        sas_token = os.environ.get("ADLS_SAS_TOKEN")
    else:
        sas_token=readAzureSecret(key_vault_name, secret_name)


    # Define el path de origen en formato WASB (Azure Blob Storage)
    # Autoloader también soporta el formato ABFS (Azure Data Lake Gen2)
    source_path_wasb = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/{path}"

    return {
        "source_path": source_path_wasb,
        "sas_token": sas_token,
        "storage_account": storage_account,
        "container_name": container_name
    }

def configure_sas_access(spark, sas_details):
    """
    Configura la propiedad de Spark para usar la SAS en el contenedor específico.
    """
    
    # Propiedad de Spark para autenticar con SAS a nivel de contenedor/cuenta
    sas_config_key = f"fs.azure.sas.{sas_details['container_name']}.{sas_details['storage_account']}.blob.core.windows.net"
    
    spark.conf.set(sas_config_key, sas_details['sas_token'])
    
    print(f"Configuración SAS para {sas_details['storage_account']}/{sas_details['container_name']} aplicada.")
    print("Auto Loader puede comenzar la lectura.")