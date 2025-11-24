import json
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient

import logging
# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def readJsonFromBlob(storage_account_name: str, container: str, blob_path: str, access_key: str) -> dict:
    """
    Lee un archivo JSON desde un contenedor en Azure Blob Storage usando DefaultAzureCredential.
    Devuelve un diccionario Python.
    """

    url = f"https://{storage_account_name}.blob.core.windows.net/{container}/{blob_path}"

    if access_key:
        credential=access_key
    else:
        # Autenticación automatizada (Managed Identity / SPN / CLI / Databricks)
        credential = DefaultAzureCredential()

    blob_client = BlobClient.from_blob_url(
        blob_url=url,
        credential=credential
    )

    try:
        # Descargar el blob completo
        raw = blob_client.download_blob().readall()
        text = raw.decode("utf-8")

        # Parsear JSON a dict
        return json.loads(text)

    except Exception as e:
        raise RuntimeError(f"Error leyendo {blob_path} desde {container}: {e}")


def readJsonFromBlobWithSas(storage_account_name: str, container_name: str, blob_path: str, sas_token: str) -> dict:
    """
    Lee un archivo JSON de configuración desde Azure Blob Storage usando un token SAS.

    :param storage_account_name: Nombre de la cuenta de almacenamiento (ej: 'tuaccountdlgen2').
    :param container_name: Nombre del contenedor/blob donde reside el fichero.
    :param blob_path: Ruta relativa del fichero JSON dentro del contenedor (ej: 'config/etl_config.json').
    :param sas_token: El Token de Firma de Acceso Compartido (SAS) completo, incluyendo el signo '?' inicial.
    :return: Un diccionario Python resultante de parsear el JSON.
    :raises RuntimeError: Si hay un error al leer o parsear el JSON.
    """

    # 1. Construir la URL completa del blob, incluyendo el token SAS como parámetro de consulta
    # Ejemplo de URL: https://[storage].blob.core.windows.net/[container]/[path/to/file]?sv=...&sig=...
    blob_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{blob_path}?{sas_token}"
    
    # 2. Inicializar el cliente del blob
    # Al pasar la URL completa con el token SAS, el cliente usa la SAS para autenticarse automáticamente.
    blob_client = BlobClient.from_blob_url(
        blob_url=blob_url
    )

    try:
        print(f"Intentando descargar la configuración desde: {blob_path}...")
        
        # 3. Descargar el contenido del blob (lectura estática y única)
        # El método download_blob descarga el contenido del blob.
        download_stream = blob_client.download_blob(max_concurrency=1)
        raw_data = download_stream.readall()
        
        # 4. Decodificar a texto (UTF-8)
        text = raw_data.decode("utf-8")

        # 5. Parsear JSON a diccionario de Python
        config_dict = json.loads(text)
        
        print("Configuración JSON leída y parseada exitosamente.")
        return config_dict

    except Exception as e:
        # Capturar cualquier error (conexión, 404 Not Found, fallo de parsing JSON, etc.)
        # y levantar una excepción clara.
        raise RuntimeError(f"ERROR al leer la configuración JSON en '{blob_path}' desde Azure Blob Storage (SAS): {e}")