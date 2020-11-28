import os
from gcloud import storage
from google.cloud import bigquery
import json

# path_to_service_account_file = "C:\\Users\\agopalda\\PycharmProjects\\GCP_Python\\gcppython1508-f6eb946b0301.json"
# local_file_path = 'C:\\Users\\agopalda\\PycharmProjects\\GCP_Python\\Copy1.doc'
# file_to_load_final = 'C:\\Users\\agopalda\\PycharmProjects\\GCP_Python\\UniversalBank.csv'
# GAC = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
# print(GAC)


class GCBQ:
    def __init__(self, client):
        self.client = client

    # def getCreds(self, path):
    #     credentials = service_account.Credentials.from_service_account_file(path, scopes=[
    #         "https://www.googleapis.com/auth/cloud-platform"], )
    #     return credentials

    def getProjectId(self, jsonfile):
        with open(jsonfile, ) as jsonob:
            getdata = json.load(jsonob)
            print(getdata['project_id'])
        return getdata['project_id']

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

    def loaddataIntoBQ(self, client, file_to_load, mybucket, tableId, dataset_id, file_to_load_final):

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
        )
        # job = client.load_table_from_uri(mybucket.get_blob("{}.csv".format(tableId)).public_url, "{}.{}.{}".format(client.project, dataset_id, tableId), job_config=job_config)
        with open(file_to_load_final, 'rb') as fload:
            job = client.load_table_from_file(fload,
                                              "{}.{}.{}".format(client.project, dataset_id, tableId),
                                              job_config=job_config)

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
