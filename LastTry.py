from azure.storage.blob import ContainerClient,BlobClient
from io import StringIO
import pandas as pd
import pickle

conn_str = 'DefaultEndpointsProtocol=https;AccountName=csb10032002646c0470;AccountKey=+5hnvPp+WfxdXdh07gtd/rEAZHKKDtqIhBm0FJI/ZpF5A6OPXqgHeN3krzdKmaDQ6ld0MOZgMAT0+AStn1sJBA==;EndpointSuffix=core.windows.net'
container = "input"
blob_name = "iris.csv"
containerName = "output"
outputBlobName = "iris_setosa2.csv"
container_client = ContainerClient.from_connection_string(
    conn_str=conn_str,
    container_name=container
    )
# Download blob as StorageStreamDownloader object (stored in memory)
downloaded_blob = container_client.download_blob(blob_name)

df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))

blob_client = container_client.get_blob_client("iris.csv")

df = df[df['Species'] == "setosa"]
# Save the blob data to a pickle file.
#blob_data = pd.read_csv('data.csv')
#b = pickle.dump(df, open('data.pkl', 'wb'))
#b = df.to_pickle()
# Establish connection with the blob storage account
blob = BlobClient.from_connection_string(conn_str=conn_str, container_name=containerName, blob_name=outputBlobName)

# Save the subset of the iris dataframe locally in task node
#df.to_csv(outputBlobName, index = False)
#########eternal code#############
# Save the subset of the iris dataframe locally in task node
#df.to_csv('data.pkl', index = False)

pd.to_pickle(df, "dummy.pkl")
with open(file="dummy.pkl", mode="rb") as data:
    blob.upload_blob(data)