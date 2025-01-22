import paramiko
import datetime
import pytz

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