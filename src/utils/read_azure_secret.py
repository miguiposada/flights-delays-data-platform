from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

import logging
# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
def readAzureSecret(key_vault_name: str, secret_name: str) -> str:
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