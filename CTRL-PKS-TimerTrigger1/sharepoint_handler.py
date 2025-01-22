# sharepoint_handler.py

import os
import urllib.parse
import logging
import requests
import json
import msal

class SharePointHandler:
    def __init__(self, log_func=None):
        """
        Initializes the SharePointHandler with necessary configurations.
        """
        self.log = log_func if log_func else logging.info
        self.tenant_id = os.getenv("TENANT_ID")
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")
        self.sharepoint_site_url = os.getenv("SHAREPOINT_SITE")  # e.g., 'https://sitasuomi.sharepoint.com/sites/Intra'
        
        if not all([self.tenant_id, self.client_id, self.client_secret, self.sharepoint_site_url]):
            raise ValueError("One or more required environment variables are missing.")
        
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
        parts = self.sharepoint_site_url.replace("https://", "").split("/sites/")
        
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
        target_drive = next((drive for drive in drives if drive["name"] == target_drive_name), None)
        
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
            folder_path (str, optional): The path to the folder within the drive. Defaults to "" (root).

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
            self.log(f"Retrieved {len(files)} item(s) from '{folder_path if folder_path else 'root'}'.")
            return files
        else:
            error_msg = f"Failed to list files: {response.status_code}, {response.text}"
            self.log(error_msg)
            raise Exception(error_msg)
    
    def move_file_to_archive(self, file_name, archive_folder="Arkisto"):
        """
        Moves an existing XLSX file to the 'Arkisto' (Archive) folder.

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
        self.create_folder_if_not_exists(archive_folder)
        
        # URL-encode the archive folder path
        encoded_archive_folder = urllib.parse.quote(archive_folder)
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/root:/Arkisto"
        
        # Rename (move) the file
        payload = {
            "parentReference": {
                "path": f"/drive/root:/{encoded_archive_folder}"
            },
            "name": file_name  # Keeps the same name
        }
        
        # Get the item ID of the file to move
        items = self.list_files(folder_path="")
        target_item = next((item for item in items if item["name"] == file_name), None)
        
        if not target_item:
            error_msg = f"File named '{file_name}' not found in the current folder."
            self.log(error_msg)
            raise Exception(error_msg)
        
        item_id = target_item["id"]
        move_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/items/{item_id}"
        
        self.log(f"Moving file '{file_name}' to '{archive_folder}'.")
        
        response = requests.patch(move_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            self.log(f"File '{file_name}' moved to '{archive_folder}' successfully.")
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
            self.log(f"File '{file_name}' uploaded successfully to '{destination_folder}'.")
            return response.json()
        else:
            error_msg = f"Failed to upload file '{file_name}': {response.status_code}, {response.text}"
            self.log(error_msg)
            raise Exception(error_msg)
    
    def create_folder_if_not_exists(self, folder_path):
        """
        Creates a folder in SharePoint if it doesn't already exist.

        Args:
            folder_path (str): The path to the folder to create.
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
