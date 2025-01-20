import datetime
import logging
import azure.functions as func
import os
import tempfile
import paramiko
import pandas as pd
import pytz
import traceback

def main(mytimer: func.TimerRequest) -> None:
    # We'll collect all logs in this list:
    log_messages = []

    def log(msg: str):
        """Append a log string to our in-memory list of messages."""
        log_messages.append(msg)
    
    try:
        # -----------------------------
        # 1. MAIN LOGIC STARTS
        # -----------------------------

        # 1A. Timer logic
        utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
        if mytimer.past_due:
            log("Timer is past due.")
        
        # 1B. Connect via Paramiko (wrapped in a helper class, see below)
        vitec = SFTP(
            hostname=os.getenv("vitec_hostname"),
            username=os.getenv("vitec_username"),
            password=os.getenv("vitec_password"),
            port=int(os.getenv("vitec_port", 22)),
            log_func=log  # pass in our log function
        )

        # 1C. Navigate to "JANNE/vantaa_tallenna_liite"
        vitec.cwd("JANNE")
        vitec.cwd("vantaa_tallenna_liite")

        # 1D. List CSV files
        csvlistdir = vitec.listdir()
        csv_files = [file for file in csvlistdir if file.endswith('.csv')]
        if not csv_files:
            log("No .csv files found. Terminating...")
            vitec.disconnect()
            # Once done with everything successfully, output final log.
            logging.info("\n".join(log_messages))
            return

        log(f"Found {len(csv_files)} CSV file(s): {csv_files}")

        local_paths = []
        for csv_file in csv_files:
            local_path = os.path.join(tempfile.gettempdir(), csv_file)
            vitec.get(csv_file, local_path)
            local_paths.append(local_path)

        # 1E. Convert CSV -> XLSX
        new_xlsx_files = []
        for local_path in local_paths:
            xlsx_path, success = convert_csv_to_xlsx(local_path, encoding='ISO-8859-1', log_func=log)
            if not success:
                # If conversion fails, stop. But still do final log.
                vitec.disconnect()
                raise RuntimeError(f"CSV-to-XLSX conversion failed for {local_path}")
            new_xlsx_files.append(xlsx_path)

        # 1F. Move original CSVs to history
        for csv_file in csv_files:
            vitec.move_files_to_history(csv_file)

        # 1G. Navigate to 'xlsx' folder and move old XLSX to history
        vitec.cwd("xlsx")
        old_xlsx_files = [f for f in vitec.listdir() if f.endswith('.xlsx')]
        if old_xlsx_files:
            log(f"Found {len(old_xlsx_files)} old .xlsx files. Moving to history: {old_xlsx_files}")
            for xlsx_file in old_xlsx_files:
                vitec.move_files_to_history(xlsx_file, add_timestamp=False)

        # 1H. Upload new XLSX files
        for xlsx_file in new_xlsx_files:
            timestamped_name = f"{vitec.get_timestamp()}_{os.path.basename(xlsx_file)}"
            vitec.put(xlsx_file, timestamped_name)

        # 1I. Disconnect and finalize
        vitec.disconnect()
        log(f"Python timer trigger function completed at {utc_timestamp}")
        
        # -----------------------------
        # 2. SUCCESS: OUTPUT LOG ONCE
        # -----------------------------
        logging.info("\n".join(log_messages))

    except Exception as e:
        # ----------------------------------------
        # 3. FAILURE: OUTPUT LOG + STACK TRACE
        # ----------------------------------------
        log_messages.append("\n--- EXCEPTION TRACEBACK ---")
        log_messages.append(traceback.format_exc())
        logging.error("\n".join(log_messages))
        # Optionally re-raise if you want the Azure Function to register as 'failed'
        raise


class SFTP:
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
                destination_path = f"history/{self.get_timestamp()}_{remote_file}"
            else:
                destination_path = f"history/{remote_file}"

            self.rename(remote_file, destination_path)
        except Exception as e:
            self.log(f"Error in move_files_to_history: {e}")
            raise

    def get_timestamp(self):
        """Return the current timestamp in the format 'YYYY-MM-DD_HH%M' in Finland timezone."""
        finland_tz = pytz.timezone('Europe/Helsinki')
        finland_time = datetime.datetime.now(finland_tz)
        return finland_time.strftime("%Y-%m-%d_%H%M")


def convert_csv_to_xlsx(csv_file_path, encoding='utf-8', log_func=None):
    """
    Converts a semicolon-delimited CSV file to an XLSX file, 
    handling special characters and European number formatting.

    Returns:
    - (xlsx_path, success_flag)
    """
    if log_func is None:
        # fallback if somehow not provided
        log_func = print
    
    success = False
    try:
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"File not found: {csv_file_path}")
        if not csv_file_path.endswith('.csv'):
            raise ValueError("Provided file is not a CSV.")

        df = pd.read_csv(csv_file_path, encoding=encoding, delimiter=';', decimal=',')
        xlsx_file_path = os.path.splitext(csv_file_path)[0] + '.xlsx'
        df.to_excel(xlsx_file_path, index=False)

        log_func(f"Converted: {csv_file_path} -> {xlsx_file_path}")
        success = True
        return xlsx_file_path, success

    except Exception as e:
        log_func(f"Error converting {csv_file_path} to XLSX: {e}")
        return "Error, no path", success
