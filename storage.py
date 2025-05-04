from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential


class AzureDataLake:
    """
    Simplified API interface to azure data lake library 
    Usage : 
        with AzureDataLake(storage_account="storage-account-name",credential="credential") as datalake:
          pass
    """
    def __init__(self,storage_account:str):
        """
        storage_account = name of the storage account
        """
        self.storage_account = storage_account
        self.service_client = None

    def __enter__(self):
        """
        Connect the data lake service client
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Clean up the data lake service client
        """
        if self.is_connected():
            self.service_client.close()

    def connect(self)->bool:
        """
        Connect to the storage account.
        Support only https connection method
        Will return false if we cannot connect to it
        """
        account_url = f"https://{self.storage_account}.dfs.core.windows.net"

        try:
            self.service_client = DataLakeServiceClient(account_url=account_url,credential=DefaultAzureCredential())
            return True
        except Exception as e:
            self.service_client = None

        return False

    def is_connected(self)->bool:
        """
        Check whether we have valid 
        """
        return self.service_client is not None
        
    def upload_file(self,container_name:str,dir_path:str,file_name:str,local_file_path:str)->bool:
        """
        Upload a file
        Usage:Consider we wanted to upload the local file called "local-test.csv" to folder1/folder2 which is in 
        container called "container-test" with the name of "remote-test.csv"
        upload_file(container_name="container-test",dir_path="folder1/folder2",file_name="remote-test.csv",local_file_path="local-test.csv")
        """
        try:

            with open(local_file_path,"rb") as local_file:
                local_data = local_file.read()

            with self.service_client.get_file_system_client(container_name) as file_system_client:

                with file_system_client.get_directory_client(dir_path) as directory_client:

                    with directory_client.get_file_client(file_name) as file_client:
                        
                        #create the file in the azure container
                        file_client.create_file()

                        #add the data into the file we have created
                        file_client.append_data(data=local_data,offset=0,length=len(local_data))

                        #flush the data back to azure data lake

                        file_client.flush_data(len(local_data))

                        return True
        
        except Exception:
            return False

    def delete_file(self,container_name:str,dir_path:str,file_name:str)->bool:
        """
        Delete a file
        Usage : Consider we wanted to delete the file called "remote-test.csv" which is in folder1/folder2 of container called
        "container-test"
        delete_file(container_name="container-test",dir_path="folder1/folder2",file_name="remote-test.csv")
        """

        try:
            with self.service_client.get_file_system_client(container_name) as file_system_client:

                with file_system_client.get_directory_client(dir_path) as directory_client:

                    with directory_client.get_file_client(file_name) as file_client:
                        
                        file_client.delete_file()

                        return True
        
        except Exception:

            return False