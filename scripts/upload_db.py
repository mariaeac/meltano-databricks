import os
from databricks.sdk import WorkspaceClient


HOST = ""
TOKEN = ""


LOCAL_PATH = ""
VOLUME_PATH = "" 


def upload_to_volumes():
    
    w = WorkspaceClient(host=HOST, token=TOKEN)

    if not os.path.exists(LOCAL_PATH):
        print("Erro: Caminho local não existe.")
        return

    
    for root, dirs, files in os.walk(LOCAL_PATH):
        for filename in files:
            if filename.startswith("."): continue

            local_file = os.path.join(root, filename)
            
           
            rel_path = os.path.relpath(local_file, LOCAL_PATH)
            dest_file = f"{VOLUME_PATH}/{rel_path}".replace("\\", "/")

            print(f"⬆ Enviando: {local_file} -> {dest_file}")
            
          
            with open(local_file, "rb") as f:
                w.files.upload(dest_file, f, overwrite=True)

    print("Upload concluído via Databricks SDK")

if __name__ == "__main__":
    upload_to_volumes()