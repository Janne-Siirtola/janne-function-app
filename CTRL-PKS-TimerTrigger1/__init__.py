# function_app.py

import datetime
import logging
import azure.functions as func
import os
import tempfile
import pandas as pd
import traceback

# Import the SharePointHandler from the sharepoint_handler module
from sharepoint_handler import SharePointHandler
from sftp_handler import SftpHandler

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
        csv_files = [file for file in csvlistdir if file.lower().endswith('.csv')]
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
            xlsx_path, success = convert_csv_to_xlsx(local_path, encoding='ISO-8859-1', log_func=log)
            if not success:
                # If conversion fails, stop. But still do final log.
                vitecSftp.disconnect()
                raise RuntimeError(f"CSV-to-XLSX conversion failed for {local_path}")
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
        main_folder = "Testi"  # Relative to Drive root
        archive_folder = f"{main_folder}/Arkisto" #"002 Vantaa/Arkisto"  # Relative to Drive root
        sharepoint.create_folder_if_not_exists(folder_path=archive_folder)

        # 2B. Move existing XLSX files in "002 Vantaa" to "Arkisto"
        # List files in "002 Vantaa"
        existing_files = sharepoint.list_files(folder_path=main_folder)
        xlsx_files_to_archive = [f["name"] for f in existing_files if f["name"].lower().endswith('.xlsx')]
        
        for xlsx_file in xlsx_files_to_archive:
            sharepoint.move_file_to_archive(file_name=xlsx_file, archive_folder=archive_folder)

        # 2C. Upload new XLSX files to "002 Vantaa"
        upload_folder = main_folder  # Destination folder in SharePoint
        for xlsx_file in new_xlsx_files:
            sharepoint.upload_file(local_file_path=xlsx_file, destination_folder=upload_folder)

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
        if not csv_file_path.lower().endswith('.csv'):
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
