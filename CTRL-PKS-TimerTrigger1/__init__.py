# function_app.py

import datetime
import logging
import azure.functions as func
import os
import tempfile
import pandas as pd
import traceback
import paramiko
import datetime
import pytz
import os
import urllib.parse
import logging
import requests
import json
import msal


def main(mytimer: func.TimerRequest) -> None:
    debug_mode = os.environ.get("DEBUG_MODE")
    # We'll collect all logs in this list:
    log_messages = []

    def log(msg: str):
        """Append a log string to our in-memory list of messages."""
        log_messages.append(msg)

    if debug_mode == "true":
        logging.info("DEBUG MODE ACTIVATED")
        debug_mode = True
    else:
        logging.info("In Production mode")
        debug_mode = False

    try:
        # -----------------------------
        # 1. MAIN LOGIC STARTS
        # -----------------------------

        # 1A. Timer logic
        utc_timestamp = datetime.datetime.utcnow().replace(
            tzinfo=datetime.timezone.utc).isoformat()
        # if mytimer.past_due:
        #    log("Timer is past due.")
        # 1B. Connect via Paramiko (SFTP for downloading CSV files)
        vitecSftp = SftpHandler(
            hostname=os.getenv("vitec_hostname"),
            username=os.getenv("vitec_username"),
            password=os.getenv("vitec_password"),
            port=int(os.getenv("vitec_port", 22)),
            log_func=log  # pass in our log function
        )

        # 1C. Navigate to "JANNE/vantaa_tallenna_liite"
        vitecSftp.cwd("JANNE")
        vitecSftp.cwd("vantaa_tallenna_liite")

        # 1D. List CSV files
        csvlistdir = vitecSftp.listdir()
        csv_files = [
            file for file in csvlistdir if file.lower().endswith('.csv')]
        if not csv_files:
            log("No .csv files found. Terminating...")
            vitecSftp.disconnect()
            # Once done with everything successfully, output final log.
            logging.info("\n".join(log_messages))
            return

        log(f"Found {len(csv_files)} CSV file(s): {csv_files}")

        local_paths = []
        for csv_file in csv_files:
            local_path = os.path.join(tempfile.gettempdir(), csv_file)
            vitecSftp.get(csv_file, local_path)
            local_paths.append(local_path)

        # 1E. Convert CSV -> XLSX
        new_xlsx_files = []
        for local_path in local_paths:
            xlsx_path, success = convert_csv_to_xlsx(
                local_path, encoding='ISO-8859-1', log_func=log)
            if not success:
                # If conversion fails, stop. But still do final log.
                vitecSftp.disconnect()
                raise RuntimeError(
                    f"CSV-to-XLSX conversion failed for {local_path}")
            new_xlsx_files.append(xlsx_path)

        # 1F. Move original CSVs to history
        for csv_file in csv_files:
            vitecSftp.move_files_to_history(csv_file)

        # -----------------------------
        # 2. SHAREPOINT HANDLING
        # -----------------------------

        # Initialize SharePointHandler with the log function
        sharepoint = SharePointHandler(log_func=log)

        # 2A. Ensure 'Arkisto' folder exists within "002 Vantaa"
        if debug_mode:
            main_folder = "Testi"  # Relative to Drive root
        else:
            main_folder = "002 Vantaa"
        # "002 Vantaa/Arkisto"  # Relative to Drive root
        archive_folder = f"{main_folder}/Arkisto"
        sharepoint.create_folder_if_not_exists(folder_path=archive_folder)

        # 2B. Move existing XLSX files in "002 Vantaa" to "Arkisto"
        # List files in "002 Vantaa"
        existing_files = sharepoint.list_files(folder_path=main_folder)
        xlsx_files_to_archive = [f["name"] for f in existing_files if f.get(
            "file") and f.get("name", "").lower().endswith('.xlsx')]

        log(f"Found {len(xlsx_files_to_archive)} existing XLSX file(s) in '{main_folder}': {xlsx_files_to_archive}")

        for xlsx_file in xlsx_files_to_archive:
            sharepoint.move_file_to_archive(
                xlsx_file, archive_folder, main_folder)

        # 2C. Upload new XLSX files to "002 Vantaa"
        upload_folder = main_folder  # Destination folder in SharePoint
        for xlsx_file in new_xlsx_files:
            sharepoint.upload_file(
                local_file_path=xlsx_file, destination_folder=upload_folder)

        # 2D. Optionally, delete temporary local XLSX files
        for xlsx_file in new_xlsx_files:
            try:
                os.remove(xlsx_file)
                log(f"Deleted temporary XLSX file: {xlsx_file}")
            except Exception as e:
                log(f"Failed to delete temporary XLSX file '{xlsx_file}': {e}")

        # -----------------------------
        # 3. Disconnect SFTP and Finalize
        # -----------------------------
        vitecSftp.disconnect()
        log(f"Python timer trigger function completed at {utc_timestamp}")

        # -----------------------------
        # 4. SUCCESS: OUTPUT LOG
        # -----------------------------
        logging.info("\n".join(log_messages))

    except Exception as e:
        # ----------------------------------------
        # 5. FAILURE: OUTPUT LOG + STACK TRACE
        # ----------------------------------------
        log_messages.append("\n--- EXCEPTION TRACEBACK ---")
        log_messages.append(traceback.format_exc())
        logging.error("\n".join(log_messages))
        # Optionally re-raise if you want the Azure Function to register as 'failed'
        raise


def get_timestamp():
    """Return the current timestamp in the format 'YYYY-MM-DD_HH%M' in Finland timezone."""
    finland_tz = pytz.timezone('Europe/Helsinki')
    finland_time = datetime.datetime.now(finland_tz)
    return finland_time.strftime("%Y-%m-%d_%H%M")


def convert_csv_to_xlsx(csv_file_path, encoding='utf-8', log_func=None):
    """
    Converts a semicolon-delimited CSV file to an XLSX file, 
    handling special characters and European number formatting.

    The XLSX filename will have a timestamp prepended to the original CSV filename.

    Example:
        Input CSV: data.csv
        Output XLSX: 2025-01-21_1230_data.xlsx

    Returns:
    - (xlsx_path, success_flag)
    """
    if log_func is None:
        # Fallback if no log function is provided
        log_func = print

    success = False
    try:
        # Validate CSV file existence
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"File not found: {csv_file_path}")

        # Validate file extension
        if not csv_file_path.lower().endswith('.csv'):
            raise ValueError("Provided file is not a CSV.")

        # Read the CSV file with specified encoding and delimiter
        df = pd.read_csv(csv_file_path, encoding=encoding,
                         delimiter=';', decimal=',')

        # Retrieve the current timestamp
        timestamp = get_timestamp()

        # Extract the base name of the CSV file (e.g., 'data' from 'data.csv')
        base_name = os.path.splitext(os.path.basename(csv_file_path))[0]

        # Create the new XLSX filename with the timestamp
        xlsx_file_name = f"{timestamp}_{base_name}.xlsx"

        # Generate the full path for the new XLSX file in the same directory as the CSV
        xlsx_file_path = os.path.join(
            os.path.dirname(csv_file_path), xlsx_file_name)

        # Write the DataFrame to an XLSX file
        df.to_excel(xlsx_file_path, index=False)

        # Log the successful conversion
        log_func(f"Converted: {csv_file_path} -> {xlsx_file_path}")
        success = True
        return xlsx_file_path, success

    except Exception as e:
        # Log any errors that occur during conversion
        log_func(f"Error converting {csv_file_path} to XLSX: {e}")
        return "Error, no path", success


class SftpHandler:
    def __init__(self, hostname: str, username: str, password: str, port: int, log_func):
        self.log = log_func
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.ssh_client = None
        self.sftp_client = None
        self.connect()

    def connect(self):
        try:
            self.log("Connecting to SFTP...")
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                hostname=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                look_for_keys=False,
                allow_agent=False,
                banner_timeout=200,
            )
            self.sftp_client = self.ssh_client.open_sftp()
            self.log("Connection successfully established via Paramiko!")
        except Exception as e:
            self.log(f"Error in connect: {e}")
            raise

    def disconnect(self):
        """Close the SFTP connection."""
        try:
            if self.sftp_client:
                self.sftp_client.close()
            if self.ssh_client:
                self.ssh_client.close()
            self.log("Connection closed.")
        except Exception as e:
            self.log(f"Error in disconnect: {e}")
            raise

    def cwd(self, remote_directory: str):
        """Change the working directory on the remote server."""
        try:
            self.sftp_client.chdir(remote_directory)
            self.log(f"Changed remote directory to: {remote_directory}")
        except Exception as e:
            self.log(f"Error in cwd({remote_directory}): {e}")
            raise

    def listdir(self):
        """List files in the current directory on the remote server."""
        try:
            files = self.sftp_client.listdir()
            self.log(f"Directory listing: {files}")
            return files
        except Exception as e:
            self.log(f"Error in listdir: {e}")
            raise

    def get(self, remote_file: str, local_file: str):
        """Download a file from the remote server."""
        try:
            self.sftp_client.get(remote_file, local_file)
            self.log(f"Downloaded: {remote_file} -> {local_file}")
        except Exception as e:
            self.log(f"Error in get: {e}")
            raise

    def put(self, local_file: str, remote_file: str):
        """Upload a file to the remote server."""
        try:
            self.sftp_client.put(local_file, remote_file)
            self.log(f"Uploaded: {local_file} -> {remote_file}")
        except Exception as e:
            self.log(f"Error in put: {e}")
            raise

    def remove(self, remote_file: str):
        """Remove a file from the remote server."""
        try:
            self.sftp_client.remove(remote_file)
            self.log(f"Removed remote file: {remote_file}")
        except Exception as e:
            self.log(f"Error in remove: {e}")
            raise

    def rename(self, source_path: str, destination_path: str):
        """Rename (or move) a file on the remote server."""
        try:
            self.sftp_client.rename(source_path, destination_path)
            self.log(f"Moved/Renamed: {source_path} -> {destination_path}")
        except Exception as e:
            self.log(f"Error in rename: {e}")
            raise

    def move_files_to_history(self, remote_file: str, add_timestamp=True):
        """Move files to the 'history' directory on the remote server."""
        try:
            files = self.listdir()
            if 'history' not in files:
                self.sftp_client.mkdir('history')
                self.log("Created 'history' directory on remote.")

            if add_timestamp:
                destination_path = f"history/{get_timestamp()}_{remote_file}"
            else:
                destination_path = f"history/{remote_file}"

            self.rename(remote_file, destination_path)
        except Exception as e:
            self.log(f"Error in move_files_to_history: {e}")
            raise


# sharepoint_handler.py


class SharePointHandler:
    def __init__(self, log_func=None):
        """
        Initializes the SharePointHandler with necessary configurations.
        """
        self.log = log_func if log_func else logging.info
        self.tenant_id = os.getenv("TENANT_ID")
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        # e.g., 'https://sitasuomi.sharepoint.com/sites/Intra'
        self.sharepoint_site_url = os.getenv("SHAREPOINT_SITE")

        if not all([self.tenant_id, self.client_id, self.client_secret, self.sharepoint_site_url]):
            raise ValueError(
                "One or more required environment variables are missing.")

        self.access_token = self.get_access_token()
        self.site_id, self.drive_id = self.get_site_and_drive_ids()

    def get_access_token(self):
        """
        Acquires an access token using client credentials flow.
        """
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        scope = ["https://graph.microsoft.com/.default"]

        app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            authority=authority,
            client_credential=self.client_secret,
        )

        result = app.acquire_token_for_client(scopes=scope)

        if "access_token" in result:
            self.log("Acquired access token successfully.")
            return result["access_token"]
        else:
            error_msg = f"Failed to acquire token: {result.get('error')}, {result.get('error_description')}"
            self.log(error_msg)
            raise Exception(error_msg)

    def get_site_and_drive_ids(self):
        """
        Retrieves the site ID and drive ID for the specified SharePoint site.
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }

        # Extract hostname and site path
        parts = self.sharepoint_site_url.replace(
            "https://", "").split("/sites/")

        if len(parts) > 1:
            hostname = parts[0]
            site_path = parts[1]
            # URL encode the site_path
            encoded_site_path = urllib.parse.quote(site_path)
            url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{encoded_site_path}"
        else:
            # Assuming it's the root site
            hostname = parts[0].rstrip('/')
            url = "https://graph.microsoft.com/v1.0/sites/root"

        self.log(f"Requesting site information from URL: {url}")

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            error_msg = f"Failed to get site ID: {response.status_code}, {response.text}"
            self.log(error_msg)
            raise Exception(error_msg)

        site_info = response.json()
        site_id = site_info["id"]

        # Now, list all drives and find the one named 'Vingo Kyselyt'
        drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        self.log(f"Fetching drives from URL: {drives_url}")
        drives_response = requests.get(drives_url, headers=headers)

        if drives_response.status_code != 200:
            error_msg = f"Failed to list drives: {drives_response.status_code}, {drives_response.text}"
            self.log(error_msg)
            raise Exception(error_msg)

        drives = drives_response.json().get("value", [])
        drive_names = [drive['name'] for drive in drives]
        self.log(f"Available drives: {drive_names}")

        # Find the drive with the name 'Vingo Kyselyt'
        target_drive_name = "Vingo Kyselyt"
        target_drive = next(
            (drive for drive in drives if drive["name"] == target_drive_name), None)

        if not target_drive:
            error_msg = f"Drive named '{target_drive_name}' not found."
            self.log(error_msg)
            raise Exception(error_msg)

        drive_id = target_drive["id"]
        self.log(f"Selected Drive ID: {drive_id} (Name: {target_drive_name})")

        return site_id, drive_id

    def list_files(self, folder_path=""):
        """
        Lists files in a specified SharePoint folder.

        Args:
            folder_path (str, optional): The path to the folder within the drive. Defaults to "" (root).----------------------------------------------------------------------------------------

        Returns:
            list: A list of file and folder objects.
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }

        if folder_path:
            # URL-encode the folder path to handle spaces and special characters
            encoded_folder_path = urllib.parse.quote(folder_path)
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/root:/{encoded_folder_path}:/children"
        else:
            # List files in the root folder
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/root/children"

        self.log(f"Listing files with URL: {url}")

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            files = response.json().get("value", [])
            self.log(
                f"Retrieved {len(files)} item(s) from '{folder_path if folder_path else 'root'}'.")
            return files
        else:
            error_msg = f"Failed to list files: {response.status_code}, {response.text}"
            self.log(error_msg)
            raise Exception(error_msg)

    def move_file_to_archive(self, file_name, archive_folder, main_folder):
        """
        Moves an existing XLSX file to the 'Arkisto' (Archive) folder.------------------------------------------------------------------------------------------------------------------------

        Args:
            file_name (str): The name of the file to move.
            archive_folder (str, optional): The destination archive folder. Defaults to "Arkisto".

        Returns:
            dict: The JSON response from the Graph API containing updated file details.
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        # Ensure the archive folder exists
        # self.create_folder_if_not_exists(archive_folder)

        # URL-encode the archive folder path
        encoded_archive_folder = urllib.parse.quote(archive_folder)
        encoded_main_folder = urllib.parse.quote(main_folder)
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/root:/{encoded_archive_folder}"

        # Rename (move) the file
        payload = {
            "parentReference": {
                "path": f"/drive/root:/{encoded_archive_folder}"
            },
            "name": file_name  # Keeps the same name
        }

        # Get the item ID of the file to move
        items = self.list_files(folder_path=main_folder)
        target_item = next(
            (item for item in items if item["name"] == file_name), None)

        if not target_item:
            error_msg = f"File named '{file_name}' not found in the current folder."
            self.log(error_msg)
            raise Exception(error_msg)

        item_id = target_item["id"]
        move_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/items/{item_id}"

        self.log(f"Moving file '{file_name}' to '{archive_folder}'.")

        response = requests.patch(move_url, headers=headers, json=payload)

        if response.status_code == 200:
            self.log(
                f"File '{file_name}' moved to '{archive_folder}' successfully.")
            return response.json()
        else:
            error_msg = f"Failed to move file '{file_name}': {response.status_code}, {response.text}"
            self.log(error_msg)
            raise Exception(error_msg)

    def upload_file(self, local_file_path, destination_folder="002 Vantaa"):
        """
        Uploads a new XLSX file to SharePoint.

        Args:
            local_file_path (str): The local path to the XLSX file.
            destination_folder (str, optional): The destination folder in SharePoint. Defaults to "002 Vantaa".

        Returns:
            dict: The JSON response from the Graph API containing file details.
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/octet-stream"
        }

        file_name = os.path.basename(local_file_path)

        # URL-encode the destination folder path
        encoded_destination_folder = urllib.parse.quote(destination_folder)
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/root:/{encoded_destination_folder}/{file_name}:/content"

        self.log(f"Uploading file '{file_name}' to '{destination_folder}'.")

        with open(local_file_path, 'rb') as f:
            file_content = f.read()

        response = requests.put(url, headers=headers, data=file_content)

        if response.status_code in [200, 201]:
            self.log(
                f"File '{file_name}' uploaded successfully to '{destination_folder}'.")
            return response.json()
        else:
            error_msg = f"Failed to upload file '{file_name}': {response.status_code}, {response.text}"
            self.log(error_msg)
            raise Exception(error_msg)

    def create_folder_if_not_exists(self, folder_path):
        """
        Creates a folder in SharePoint if it doesn't already exist.

        Args:
            folder_path (str): The path to the folder to create.---------------------------------------------------------------------------------------------------------
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        # Check if the folder already exists
        try:
            self.list_files(folder_path)
            self.log(f"Folder '{folder_path}' already exists.")
        except Exception as e:
            if "itemNotFound" in str(e):
                # Folder does not exist, create it
                encoded_folder_path = urllib.parse.quote(folder_path)
                url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/root:/{encoded_folder_path}:/children"
                payload = {
                    "name": folder_path.split('/')[-1],
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "fail"
                }
                self.log(f"Creating folder '{folder_path}'.")
                response = requests.post(url, headers=headers, json=payload)
                if response.status_code in [200, 201]:
                    self.log(f"Folder '{folder_path}' created successfully.")
                else:
                    error_msg = f"Failed to create folder '{folder_path}': {response.status_code}, {response.text}"
                    self.log(error_msg)
                    raise Exception(error_msg)
            else:
                # Other exceptions
                raise e
