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

    source_path = 'home/ubuntu/smrtlink/userdata/jobs_root/0000/0000000/0000000617/outputs/fastx_files'
    
    splits= Path(source_path).parts

    s3_client = boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    bucket_name = 'sequencingrunslatchbio'

    Bucket_path = f'0000/0000000/{splits[7]}/{splits[8]}/{splits[9]}'

    days_threshold = 30
    # minutes_threshold = 1

    current_time = datetime.now(timezone.utc)
    print(current_time)

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
                
    while objects_to_delete:
        batch = objects_to_delete[:1000]
        response = s3_client.delete_objects(Bucket = bucket_name, Delete = {'Objects' : batch})
        
        if 'Deleted' in response:
            deleted_objects = response['Deleted']
            print (f"Deleted {len(deleted_objects)} objects")
            objects_to_delete = objects_to_delete[1000:]
        else:
            break

    #  code to transfer the files from Ec2 instance to the S3 bucket

    ec2_client = boto3.client('ec2', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    response = ec2_client.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': [instance_name]}])
    instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']

    response = ec2_client.describe_instances(InstanceIds=[instance_id])
    private_ip = response['Reservations'][0]['Instances'][0]['PrivateIpAddress']
    public_ip = response['Reservations'][0]['Instances'][0]['PublicIpAddress']

    ssh_username = 'ubuntu'
    ssh_password = 'gl*ekofUbr8jo69ufuch'

    ssh_client = paramiko.SSHClient()

    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(public_ip, username = ssh_username, password= ssh_password, timeout= 10000.0)

    # temp_directory = 'ab1/tmp/files/'
    # os.makedirs(temp_directory)

    sftp_client = ssh_client.open_sftp()
    remote_files = sftp_client.listdir(f'/{source_path}')


    for file_name in remote_files:
        if file_name.endswith(file_format):
            temp_directory = 'PacBio/tmp/files/'
            os.makedirs(temp_directory)
            remote_file_path = f'/{source_path}/{file_name}'
            local_file_path = os.path.join(temp_directory, file_name)
            sftp_client.get(remote_file_path, local_file_path)
            bucket_path = f'{Bucket_path}/{file_name}'
            s3_client.upload_file(local_file_path, bucket_name, Key = bucket_path)
            print(f'The file: {file_name} is Uploaded to {bucket_name}')
            shutil.rmtree(temp_directory)
            
    sftp_client.close()
    ssh_client.close()

    shutil.rmtree(temp_directory)

    s3_url = f"https://s3.console.aws.amazon.com/s3/buckets/{bucket_name}?region={region}&tab=objects"
    print(f"Data transferred to S3: {s3_url}")
    return 

Ec2_to_s3()





        
