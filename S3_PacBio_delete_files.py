from datetime import datetime, timedelta,timezone
import boto3


aws_access_key_id ='AKIA52UT325RXFZOC45A'
aws_secret_access_key = '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'
region = 'us-east-1'

bucket_name ='abdata'
bucket_path = '00001/00002/00003/files/'

s3_client = boto3.client('s3',aws_access_key_id = aws_access_key_id, 
                         aws_secret_access_key = aws_secret_access_key, region_name = region )


days_threshold = 1
hour_threshold = 1
current_time = datetime.now(timezone.utc)
print(current_time)
threshold_time = current_time - timedelta( hours= hour_threshold)
print(threshold_time)
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix= bucket_path)
files_to_delete =[]

if 'Contents' in response:
    for obj in response['Contents']:
        key = obj['Key']
        last_modified = obj['LastModified']
        if last_modified < threshold_time:
            files_to_delete.append({'Key' : key})
            
while files_to_delete:
    batch = files_to_delete[:1000]
    response = s3_client.delete_objects(Bucket = bucket_name, Delete = {'Objects' : batch})
    if 'Deleted' in response:
        deleted_objects = response['Deleted']
        print(f' Deleted {len(deleted_objects)} objects')
        files_to_delete = files_to_delete[1000:]
    else:
        break
        
    
    
            
