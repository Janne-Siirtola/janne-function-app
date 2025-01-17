import datetime
import logging
import azure.functions as func
import os
import tempfile
import paramiko 
import pandas as pd
import pytz


class SFTP:
    def __init__(self, hostname: str, username: str, password: str, port: int):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.ssh_client = None
        self.sftp_client = None
        self.connect()

    def connect(self):
        try:
            # Turn on debug logging
            logging.basicConfig(level=logging.DEBUG)

            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            self.ssh_client.connect(
                hostname=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                look_for_keys=False,
                allow_agent=False,
                banner_timeout=200,  # in case the server is slow to present auth banner
            )

            self.sftp_client = self.ssh_client.open_sftp()
            logging.info("Connection successfully established via Paramiko!")
        except Exception as e:
            logging.error(f"Error in connect: {e}")
            raise

    def disconnect(self):
        """Close the SFTP connection."""
        try:
            if self.sftp_client:
                self.sftp_client.close()
            if self.ssh_client:
                self.ssh_client.close()
            logging.info("Connection closed.")
        except Exception as e:
            logging.error(f"Error in disconnect: {e}")
            raise

    def cwd(self, remote_directory: str):
        """Change the working directory on the remote server."""
        try:
            self.sftp_client.chdir(remote_directory)
            logging.info(f"Remote directory changed to {remote_directory}")
        except Exception as e:
            logging.error(f"Error in cwd: {e}")
            raise

    def get(self, remote_file: str, local_file: str):
        """Download a file from the remote server."""
        try:
            self.sftp_client.get(remote_file, local_file)
            logging.info(f"File {remote_file} downloaded as {local_file}")
        except Exception as e:
            logging.error(f"Error in get: {e}")
            raise

    def put(self, local_file: str, remote_file: str):
        """Upload a file to the remote server."""
        try:
            self.sftp_client.put(local_file, remote_file)
            logging.info(f"File {local_file} uploaded as {remote_file}")
        except Exception as e:
            logging.error(f"Error in put: {e}")
            raise

    def listdir(self):
        """List files in the current directory on the remote server."""
        try:
            return self.sftp_client.listdir()
        except Exception as e:
            logging.error(f"Error in listdir: {e}")
            raise

    def remove(self, remote_file: str):
        """Remove a file from the remote server."""
        try:
            self.sftp_client.remove(remote_file)
            logging.info(f"File {remote_file} removed from the server")
        except Exception as e:
            logging.error(f"Error in remove: {e}")
            raise

    def rename(self, source_path: str, destination_path: str):
        """Rename (or move) a file on the remote server."""
        try:
            self.sftp_client.rename(source_path, destination_path)
            logging.info(f"File moved from {source_path} to {destination_path}")
        except Exception as e:
            logging.error(f"Error in rename: {e}")
            raise

    def move_files_to_history(self, remote_file: str, add_timestamp=True):
        """Move files to the 'history' directory on the remote server."""
        try:
            listdir = self.listdir()
            if 'history' not in listdir:
                self.sftp_client.mkdir('history')
                logging.info("Created 'history' directory.")

            source_path = remote_file

            if add_timestamp:
                destination_path = f"history/{self.get_timestamp()}_{remote_file}"
            else:
                destination_path = f"history/{remote_file}"

            logging.info(f"Renaming {source_path} to {destination_path}")
            self.rename(source_path, destination_path)
        except Exception as e:
            logging.error(f"Error in move_files_to_history: {e}")
            raise

    def get_timestamp(self):
        """Return the current timestamp in the format 'YYYY-MM-DD_HHMM' in Finland timezone."""
        finland_tz = pytz.timezone('Europe/Helsinki')
        finland_time = datetime.datetime.now(finland_tz)
        return finland_time.strftime("%Y-%m-%d_%H%M")


def convert_csv_to_xlsx(csv_file_path, encoding='utf-8'):
    """
    Converts a semicolon-delimited CSV file to an XLSX file, handling special characters and European number formatting.

    Parameters:
    - csv_file_path (str): Path to the CSV file.
    - encoding (str): Encoding of the CSV file (default is 'utf-8').

    Returns:
    - str: Path to the generated XLSX file.
    """
    try:
        # Ensure the file exists and has a .csv extension
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"File not found: {csv_file_path}")
        if not csv_file_path.endswith('.csv'):
            raise ValueError("The provided file is not a CSV file.")

        # Read the CSV file with semicolon delimiter and correct encoding
        df = pd.read_csv(csv_file_path, encoding=encoding, delimiter=';', decimal=',')

        # Generate the XLSX file path
        xlsx_file_path = os.path.splitext(csv_file_path)[0] + '.xlsx'

        # Save as XLSX
        df.to_excel(xlsx_file_path, index=False)
        logging.info(f"Converted {csv_file_path} to {xlsx_file_path}")
        return xlsx_file_path

    except Exception as e:
        logging.error(f"Error during conversion: {e}")
        return None


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc
    ).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    # Create SFTP instance with Paramiko
    vitec = SFTP(
        os.getenv("vitec_hostname"),
        os.getenv("vitec_username"),
        os.getenv("vitec_password"),
        int(os.getenv("vitec_port", 22))  # ensure port is int
    )

    # Navigate to the correct directory
    vitec.cwd("JANNE")
    vitec.cwd("vantaa_tallenna_liite")

    # List available CSV files
    csvlistdir = vitec.listdir()
    logging.info(f"Current directory listing: {csvlistdir}")

    csv_files = [file for file in csvlistdir if file.endswith('.csv')]
    new_xlsx_files = list()

    if csv_files:
        logging.info(f"{len(csv_files)} .csv file(s) found. Downloading...")
        for csv_file in csv_files:
            local_path = os.path.join(tempfile.gettempdir(), csv_file)
            vitec.get(csv_file, local_path)
            vitec.move_files_to_history(csv_file)
            xlsx_path = convert_csv_to_xlsx(local_path, encoding='ISO-8859-1')
            new_xlsx_files.append(xlsx_path)
    else:
        logging.info("No .csv files found, terminating...")
        vitec.disconnect()
        return

    # Navigate to 'xlsx' folder
    vitec.cwd("xlsx")
    xlsxlistdir = vitec.listdir()
    old_xlsx_files = [file for file in xlsxlistdir if file.endswith('.xlsx')]

    if old_xlsx_files:
        logging.info(f"{len(old_xlsx_files)} old .xlsx files found: {old_xlsx_files}")
        logging.info(f"Moving {len(old_xlsx_files)} old .xlsx files to history.")
        for xlsx_file in old_xlsx_files:
            vitec.move_files_to_history(xlsx_file, False)
    else:
        logging.info("No old .xlsx files found.")

    # Upload new XLSX files (with a timestamp prefix)
    for xlsx_file in new_xlsx_files:
        if xlsx_file:  # Ensure conversion succeeded
            vitec.put(
                xlsx_file,
                f"{vitec.get_timestamp()}_{os.path.basename(xlsx_file)}"
            )

    vitec.disconnect()
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
