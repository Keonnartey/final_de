import requests
from dotenv import load_dotenv
import os
import json
import base64

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")

print(server_h)
FILESTORE_PATH = "dbfs:/FileStore/databricks_project"
headers = {"Authorization": "Bearer %s" % access_token}
url = "https://" + server_h + "/api/2.0"


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request(
        "POST", url + path, data=json.dumps(data), verify=True, headers=headers
    )
    return resp.json()


def mkdirs(path, headers):
    _data = {}
    _data["path"] = path
    return perform_query("/dbfs/mkdirs", headers=headers, data=_data)


def create(path, overwrite, headers):
    _data = {}
    _data["path"] = path
    _data["overwrite"] = overwrite
    return perform_query("/dbfs/create", headers=headers, data=_data)


def add_block(handle, data, headers):
    _data = {}
    _data["handle"] = handle
    _data["data"] = data
    return perform_query("/dbfs/add-block", headers=headers, data=_data)


def close(handle, headers):
    _data = {}
    _data["handle"] = handle
    return perform_query("/dbfs/close", headers=headers, data=_data)

def put_file_from_local(file_path, dbfs_path, overwrite, headers):
    with open(file_path, "rb") as file:
        content = file.read()
        handle = create(dbfs_path, overwrite, headers=headers)["handle"]
        print("Putting file: " + dbfs_path)
        for i in range(0, len(content), 2 ** 20):
            add_block(
                handle,
                base64.standard_b64encode(content[i : i + 2 ** 20]).decode(),
                headers=headers,
            )
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")

def extract_local_files(src_folder, dest_folder, overwrite=True):
    files_to_extract = ["games.csv", "plays.csv", "tackles.csv", "players.csv", "La_vs_buf.csv"]

    for file_name in files_to_extract:
        local_file_path = os.path.join(src_folder, file_name)
        dbfs_file_path = os.path.join(dest_folder, file_name)
        
        put_file_from_local(local_file_path, dbfs_file_path, overwrite, headers=headers)

    return dest_folder

if __name__ == "__main__":
    src_folder = "../src"  # Update the path accordingly
    dest_folder = FILESTORE_PATH  # Destination folder in DBFS
    extract_local_files(src_folder, dest_folder)
