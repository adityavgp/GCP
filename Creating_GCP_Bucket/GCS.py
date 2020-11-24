import os
from gcloud import storage
from google.cloud import bigquery
import json

# path_to_service_account_file = "C:\\Users\\agopalda\\PycharmProjects\\GCP_Python\\gcppython1508-f6eb946b0301.json"
local_file_path = 'C:\\Users\\agopalda\\PycharmProjects\\GCP_Python\\Copy1.doc'
file_to_load_final = 'C:\\Users\\agopalda\\PycharmProjects\\GCP_Python\\UniversalBank.csv'
GAC = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')


# print(GAC)
class ConnectorGC:
    def __init__(self):
        pass

    # def getCreds(self, path):
    #     credentials = service_account.Credentials.from_service_account_file(path, scopes=[
    #         "https://www.googleapis.com/auth/cloud-platform"], )
    #     return credentials

    def initiateConnectionBQ(self):
        clientBQ = bigquery.Client()
        return clientBQ

    def initiateConnectionCloudStorage(self):
        clientCS = storage.Client()
        return clientCS

    def createGCBucket(self, clientCS, bucketname):
        bucket = clientCS.bucket(bucketname)
        bucket.location = 'asia'
        bucket.create()
        return bucket

    def getProjectId(self, jsonfile):
        with open(jsonfile, ) as jsonob:
            getdata = json.load(jsonob)
            print(getdata['project_id'])
        return getdata['project_id']

    def getGCBucket(self, clientCS, bucketname):
        try:
            bucket = clientCS.get_bucket(bucketname)
        except Exception as e:
            return None
        return bucket

    def getBQDsetId(self, dataset_id, clientBQ):
        try:
            dataset = clientBQ.get_dataset(dataset_id)
        except Exception as e:
            return None
        return dataset

    def getBQTableId(self, table_id, clientBQ):
        try:
            table = clientBQ.get_table(table_id)
        except Exception as e:
            return None
        return table

    def copylocaltoGCPBucket(self, local_file_path, mybucket):
        blob = mybucket.blob((os.path.basename(local_file_path)))
        with open(local_file_path, 'rb') as docc:
            blob.upload_from_file(docc)
        # print(mimetypes.guess_type(local_file_path)[0])
        #
        # print(blob)
        # blob.upload_from_file(local_file_path, content_type='application/pdf')
        return True

    def createDatasetBQ(self, dataset_id, client):
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "asia-south1"
        dataset = client.create_dataset(dataset)
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
        return dataset

    def createTableBQ(self, table_id, dataset_id, client):
        schema = [
            bigquery.SchemaField("ID", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Age", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Experience", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Income", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("ZIPCode", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Family", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("CCAvg", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("Education", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Mortgage", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("PersonalLoan", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("SecuritiesAccount", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("CDAccount", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("Online", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("CreditCard", "BOOLEAN", mode="REQUIRED"),
        ]
        table = bigquery.Table("{}.{}.{}".format(client.project, dataset_id, table_id), schema=schema)
        table = client.create_table(table)  # Make an API request.
        print(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
        return table

    def loaddataIntoBQ(self, client, file_to_load, mybucket, tableId, dataset_id):

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
        )
        #job = client.load_table_from_uri(mybucket.get_blob("{}.csv".format(tableId)).public_url, "{}.{}.{}".format(client.project, dataset_id, tableId), job_config=job_config)
        with open(file_to_load_final, 'rb') as fload:
            job = client.load_table_from_file(fload,
                                         "{}.{}.{}".format(client.project, dataset_id, tableId), job_config=job_config)

        job.result()  # Waits for the job to complete.
        table = client.get_table("{}.{}.{}".format(client.project, dataset_id, tableId))  # Make an API request.
        print(table)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), tableId
            )
        )
        return True

    # def getFileURI(self, mybucket):
    #     blobs = mybucket.list_blobs()
    #     for blb in blobs:
    #         print(blb.path)

def main():
    conn = ConnectorGC()
    bucketId = 'bigqbucket1102'
    datasetId = 'universal1102'
    tableId = 'UniversalBank'
    clientCloudStorage = conn.initiateConnectionCloudStorage()
    mybucket = conn.getGCBucket(clientCloudStorage, bucketId)
    #print(mybucket)
    if mybucket is None:
        mybucket = conn.createGCBucket(clientCloudStorage, bucketId)
    fileCopied = conn.copylocaltoGCPBucket(file_to_load_final, mybucket)
    if fileCopied:
        print('File Copied to GCP Bucket {}', format(mybucket))
    #print(mybucket.list_blobs())
    clientBQ = conn.initiateConnectionBQ()
    dataset_id = conn.getBQDsetId(datasetId, clientBQ)
    #print(dataset_id)
    if dataset_id is None:
        dataset_id = conn.createDatasetBQ("{}.{}".format(clientBQ.project, datasetId), clientBQ)
    #print(dataset_id)
    table_id = conn.getBQTableId("{}.{}.{}".format(clientBQ.project, dataset_id, tableId), clientBQ)
    #print(table_id)
    if table_id is not None:
        table_id = conn.createTableBQ(tableId, datasetId, clientBQ)
    #conn.getFileURI(mybucket)
    #print(mybucket.get_blob("{}.csv".format(tableId)).public_url)
    fileLoadedToBQ = conn.loaddataIntoBQ(clientBQ, file_to_load_final, mybucket, tableId, datasetId)



# need to do -- get correct table_id and then run this
if __name__ == "__main__":
    main()
