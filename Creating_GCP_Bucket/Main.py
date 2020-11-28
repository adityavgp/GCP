from Creating_GCP_Bucket.GCS import GCStorage
from Creating_GCP_Bucket.BQ import GCBQ
from Creating_GCP_Bucket.Commons import Commons
from gcloud import storage
from google.cloud import bigquery

file_to_load_final = 'C:\\Users\\agopalda\\PycharmProjects\\GCP_Python\\UniversalBank.csv'

def main():
    connGCS = GCStorage(storage.Client())
    connBQ = GCBQ(bigquery.Client())
    bucketId = 'bigqbucket110211'
    datasetId = 'universal110211'
    tableId = 'UniversalBank'
    mybucket = connGCS.getGCBucket(connGCS.client, bucketId)
    #print(mybucket)
    if mybucket is None:
        mybucket = connGCS.createGCBucket(connGCS.client, bucketId)
    fileCopied = connGCS.copylocaltoGCPBucket(file_to_load_final, mybucket)
    if fileCopied:
        print('File Copied to GCP Bucket {}', format(mybucket))
    #print(mybucket.list_blobs())
    dataset_id = connBQ.getBQDsetId(datasetId, connBQ.client)
    #print(dataset_id)
    if dataset_id is None:
        dataset_id = connBQ.createDatasetBQ("{}.{}".format(connBQ.client.project, datasetId), connBQ.client)
    #print(dataset_id)
    table_id = connBQ.getBQTableId("{}.{}.{}".format(connBQ.client.project, dataset_id, tableId), connBQ.client)
    #print(table_id)
    if table_id is not None:
        table_id = connBQ.createTableBQ(tableId, datasetId, connBQ.client)
    #conn.getFileURI(mybucket)
    #print(mybucket.get_blob("{}.csv".format(tableId)).public_url)
    fileLoadedToBQ = connBQ.loaddataIntoBQ(connBQ.client, file_to_load_final, mybucket, tableId, datasetId, file_to_load_final)



# need to do -- get correct table_id and then run this
if __name__ == "__main__":
    main()
