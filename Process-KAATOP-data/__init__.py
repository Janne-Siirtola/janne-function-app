import datetime
import logging
import os
import traceback
import tempfile

import azure.functions as func
import pandas as pd
from datetime import datetime
import pytz
import paramiko


def main(mytimer: func.TimerRequest) -> None:
    
    DEBUG_MODE = os.environ.get("DEBUG_MODE")
    
    # We'll collect all logs in this list:
    log_messages = []
    
    temp_dir = tempfile.gettempdir()

    def log(msg: str):
        """Append a log string to our in-memory list of messages."""
        log_messages.append(msg)

    # Determine debug mode from environment variable
    if DEBUG_MODE == "true":
        logging.info("-----IN DEBUG MODE-----")
        log("-----IN DEBUG MODE-----")
        DEBUG_MODE = True
    else:
        logging.info("-----IN PRODUCTION MODE-----")
        log("-----IN PRODUCTION MODE-----")
        DEBUG_MODE = False
        
    try:
        # Initialize SFTP handler
        vitecSftp = SftpHandler(
            hostname=os.getenv("vitec_hostname"),
            username=os.getenv("vitec_username"),
            password=os.getenv("vitec_password"),
            port=int(os.getenv("vitec_port", 22)),
            log_func=log  # pass in our log function
        )
        
        # Navigate to the directory with raw CSV files
        vitecSftp.cwd("jhl_vastaanottopaikat")
        vitecSftp.cwd("RAW-DATA")
        
        # List CSV files
        csvlistdir = vitecSftp.listdir()
        csv_files = [file for file in csvlistdir if file.lower().endswith('.csv')]
        if not csv_files:
            log("No .csv files found. Terminating...")
            vitecSftp.disconnect()
            logging.info("\n".join(log_messages))
            return
        
        log(f"Found {len(csv_files)} CSV file(s): {csv_files}")

        raw_paths = []
        for csv_file in csv_files:
            local_path = os.path.join(temp_dir, csv_file)
            vitecSftp.get(csv_file, local_path)
            raw_paths.append(local_path)
            vitecSftp.move_files_to_history(csv_file)
        
        processed_paths = []
        for path in raw_paths:
            # Load the original CSV data
            df = load_data(path)
            
            # Extract ID and TEXT records from the DataFrame
            id_records, text_records = extract_records(df)
            
            # Merge the extracted records on 'COMPos'
            merged_df = merge_records(id_records, text_records)
            
            # Save the merged DataFrame to a new CSV file with a timestamp in its name
            processed_paths.append(save_data(merged_df, temp_dir, log))
            
        # Change directory to PROCESSED on remote
        vitecSftp.cwd("..")
        vitecSftp.cwd("PROCESSED")
        
        # Upload each processed file to the remote server
        for path in processed_paths:
            vitecSftp.put(path, os.path.basename(path))
        
        vitecSftp.disconnect()
        log(f"Python timer trigger function completed at {get_timestamp()}")
        
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
    """
    Return the current timestamp in the format 'YYYY-MM-DD_HH%M'
    in the Finland timezone.
    """
    finland_tz = pytz.timezone('Europe/Helsinki')
    finland_time = datetime.now(finland_tz)
    return finland_time.strftime("%Y-%m-%d_%H%M")


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load CSV data from the specified file path using ISO-8859-1 encoding and semicolon delimiter.
    Drops the first row if it's not needed.
    """
    try:
        df = pd.read_csv(file_path, encoding='ISO-8859-1', delimiter=';')
        df.drop(0, inplace=True)  # Drop the first row if it's a header duplication or unwanted row
        return df
    except Exception as e:
        logging.error("Error loading data from file %s: %s", file_path, e)
        raise


def extract_records(df: pd.DataFrame):
    """
    Split the DataFrame into two separate DataFrames based on the type indicated in 'COMKey':
      - id_records: rows where 'COMKey' contains 'ID'
      - text_records: rows where 'COMKey' contains 'TEXT'
    Rename the 'COMText' column to reflect its content in each case.
    """
    try:
        # Extract rows where 'COMKey' indicates an ID
        id_records = df[df["COMKey"].str.contains("ID", na=False)][["COMPos", "COMText"]].copy()
        id_records.rename(columns={"COMText": "TAPKaatop"}, inplace=True)
    
        # Extract rows where 'COMKey' indicates a TEXT
        text_records = df[df["COMKey"].str.contains("TEXT", na=False)][["COMPos", "COMText"]].copy()
        text_records.rename(columns={"COMText": "TAPKaatopDefinition"}, inplace=True)
    
        return id_records, text_records
    except Exception as e:
        logging.error("Error extracting records: %s", e)
        raise


def merge_records(id_records: pd.DataFrame, text_records: pd.DataFrame) -> pd.DataFrame:
    """
    Merge the id_records and text_records DataFrames on the 'COMPos' column.
    Optionally, drop 'COMPos' from the final DataFrame if it's not needed.
    """
    try:
        merged_df = pd.merge(id_records, text_records, on="COMPos")
        merged_df.drop("COMPos", axis=1, inplace=True)
        return merged_df
    except Exception as e:
        logging.error("Error merging records: %s", e)
        raise


def save_data(df: pd.DataFrame, save_path: str, log):
    """
    Save the provided DataFrame as a CSV file using ISO-8859-1 encoding and semicolon as the delimiter.
    The filename will include the current timestamp followed by '_KAATOPAIKAT.csv'.
    """
    try:
        # Note the function call get_timestamp() is now correctly invoked with parentheses.
        filename = f"{get_timestamp()}_KAATOPAIKAT.csv"
        final_path = os.path.join(save_path, filename)
        df.to_csv(final_path, index=False, encoding='ISO-8859-1', sep=";")
        log(f"Data successfully saved to: {final_path}")
        return final_path
    except Exception as e:
        log(f"Error saving data: {e}")
        raise


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
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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
