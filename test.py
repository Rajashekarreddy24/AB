import boto3
import paramiko
import os
from pathlib import Path
import shutil
from datetime import datetime , timedelta, timezone


def Ec2_to_s3():
    
    region = 'us-east-1' 

    aws_access_key_id = 'AKIA5PUNWEBOKUPOHFH2'
    
    aws_secret_access_key = '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'

    instance_name = 'PacBio_SMRTLINK'

    file_format = '.gz'

    source_path = 'home/ubuntu/smrtlink/userdata/jobs_root/0000/0000000/0000000615/outputs/fastx_files'
    
    splits= Path(source_path).parts

    s3_client = boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    bucket_name = 'sequencingrunslatchbio'
    

    Bucket_path = f'0000/0000000/{splits[7]}/{splits[8]}/{splits[9]}'
    
    Backup_bucket = 'archivefroms3'

    days_threshold = 30
    
    current_time = datetime.now(timezone.utc)
    threshold_time = current_time - timedelta(days= days_threshold)
    print(threshold_time)
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix= Bucket_path)
    objects_to_delete =[]

    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            last_modified = obj['LastModified']
            if last_modified < threshold_time:
                objects_to_delete.append({'Key' : key})
                for file_key in objects_to_delete:
                    files = file_key['Key']
                    copy_source = {"Bucket" : bucket_name, "Key" : files}
                    s3_client.copy(copy_source,Bucket =Backup_bucket, Key = files)  
                    
                print(f'Total {len(files)} out of {len(objects_to_delete)} files copied successfully....')
                
    


    
Ec2_to_s3()




