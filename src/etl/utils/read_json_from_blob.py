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
