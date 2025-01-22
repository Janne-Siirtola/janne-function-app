import os
import json
import logging
import azure.functions as func
from typing import List
from azure.storage.blob import BlobServiceClient

class BlobStorageClient:
    def __init__(self, connection_string: str, container_name: str):
        """
        Initializes the BlobStorageClient with connection details.

        :param connection_string: Azure Storage connection string.
        :param container_name: Name of the blob container.
        """
        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_service_client = self._initialize_blob_service_client()
        self.container_client = self._get_container_client()

    def _initialize_blob_service_client(self) -> BlobServiceClient:
        """
        Initializes the BlobServiceClient.

        :return: BlobServiceClient instance.
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            logging.info("Initialized BlobServiceClient.")
            return blob_service_client
        except Exception as e:
            logging.error(f"Failed to initialize BlobServiceClient: {e}")
            raise

    def _get_container_client(self):
        """
        Retrieves the ContainerClient for the specified container.

        :return: ContainerClient instance.
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            logging.info(f"Accessed container: {self.container_name}")
            return container_client
        except Exception as e:
            logging.error(f"Failed to access container '{self.container_name}': {e}")
            raise

    def list_csv_blobs(self, directory_prefix: str) -> List[str]:
        """
        Lists all .csv blobs directly within the specified directory.

        :param directory_prefix: Prefix of the directory to search in.
        :return: List of .csv blob names.
        """
        try:
            blobs = self.container_client.list_blobs(name_starts_with=directory_prefix)
            logging.info(f"Retrieved blobs with prefix: {directory_prefix}")
            
            # Filter blobs directly in the directory and ending with .csv
            direct_csv_blobs = [
                blob.name for blob in blobs
                if self._is_direct_blob(blob.name, directory_prefix) and blob.name.lower().endswith('.csv')
            ]

            logging.info(f"Found {len(direct_csv_blobs)} .csv blobs directly in '{directory_prefix}'.")
            return direct_csv_blobs

        except Exception as e:
            logging.error(f"Error listing blobs: {e}")
            raise

    @staticmethod
    def _is_direct_blob(blob_name: str, directory_prefix: str) -> bool:
        """
        Determines if a blob is directly within the specified directory (no subdirectories).

        :param blob_name: Full name of the blob.
        :param directory_prefix: Directory prefix.
        :return: True if blob is directly within the directory, False otherwise.
        """
        relative_path = blob_name[len(directory_prefix):]
        is_direct = '/' not in relative_path
        if not is_direct:
            logging.debug(f"Excluded blob (subdirectory): {blob_name}")
        return is_direct

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing request to list .csv blobs in a specific directory.')

    # Retrieve configuration from environment variables
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_name = os.getenv('CONTAINER_NAME', 'vitecpowerbi')  # Default to 'vitecpowerbi' if not set
    directory_prefix = os.getenv('DIRECTORY_PREFIX', 'JANNE/vantaa_tallenna_liite/')  # Default prefix

    if not connection_string:
        logging.error("AZURE_STORAGE_CONNECTION_STRING is not set.")
        return func.HttpResponse(
            "Configuration error: AZURE_STORAGE_CONNECTION_STRING is missing.",
            status_code=500
        )

    # Optionally, allow dynamic directory input via query parameter or request body
    # If not provided, use the default directory_prefix from environment variables
    directory_input = req.params.get('directory')
    if not directory_input:
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = {}
        directory_input = req_body.get('directory', directory_prefix)

    # Ensure the prefix ends with '/'
    if not directory_input.endswith('/'):
        directory_input += '/'

    try:
        # Initialize BlobStorageClient
        blob_client = BlobStorageClient(connection_string, container_name)
    except Exception as e:
        logging.error(f"Failed to initialize BlobStorageClient: {e}")
        return func.HttpResponse(
            "Internal Server Error: Could not initialize storage client.",
            status_code=500
        )

    try:
        # Retrieve .csv blobs directly in the specified directory
        csv_blobs = blob_client.list_csv_blobs(directory_input)
    except Exception as e:
        logging.error(f"Failed to list .csv blobs: {e}")
        return func.HttpResponse(
            "Internal Server Error: Could not retrieve blobs.",
            status_code=500
        )

    if not csv_blobs:
        logging.info(f"No .csv blobs found in directory '{directory_input}'.")
        return func.HttpResponse(
            f"No .csv blobs found in directory '{directory_input}'.",
            status_code=404
        )

    # Prepare JSON response
    response_body = {
        "directory": directory_input,
        "csv_blobs": csv_blobs
    }

    return func.HttpResponse(
        json.dumps(response_body),
        mimetype="application/json",
        status_code=200
    )
