import os
# from gcloud import storage
# from google.cloud import bigquery
# import json

# # path_to_service_account_file = "C:\\Users\\agopalda\\PycharmProjects\\GCP_Python\\gcppython1508-f6eb946b0301.json"
# local_file_path = 'C:\\Users\\agopalda\\PycharmProjects\\GCP_Python\\Copy1.doc'
# file_to_load_final = 'C:\\Users\\agopalda\\PycharmProjects\\GCP_Python\\UniversalBank.csv'
# GAC = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
# print(GAC)


class GCStorage:
    def __init__(self, client):
        self.client = client

    # def getCreds(self, path):
    #     credentials = service_account.Credentials.from_service_account_file(path, scopes=[
    #         "https://www.googleapis.com/auth/cloud-platform"], )
    #     return credentials

    def createGCBucket(self, clientCS, bucketname):
        bucket = clientCS.bucket(bucketname)
        bucket.location = 'asia'
        bucket.create()
        return bucket

    def getGCBucket(self, clientCS, bucketname):
        try:
            bucket = clientCS.get_bucket(bucketname)
        except Exception as e:
            return None
        return bucket

    def copylocaltoGCPBucket(self, local_file_path, mybucket):
        blob = mybucket.blob((os.path.basename(local_file_path)))
        with open(local_file_path, 'rb') as docc:
            blob.upload_from_file(docc)
        # print(mimetypes.guess_type(local_file_path)[0])
        #
        # print(blob)
        # blob.upload_from_file(local_file_path, content_type='application/pdf')
        return True

    # def getFileURI(self, mybucket):
    #     blobs = mybucket.list_blobs()
    #     for blb in blobs:
    #         print(blb.path)