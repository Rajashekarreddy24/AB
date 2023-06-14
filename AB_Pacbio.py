
import boto3
import paramiko
import tarfile
import os

region = 'us-east-1'

aws_access_key_id = 'AKIA5PUNWEBOKUPOHFH2'
aws_secret_access_key = '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'

instance_name = 'PacBio_SMRTLINK'

bucket_name = 'sequencingrunslatchbio'

s3_prefix = ''

file_format = '.gz'

ec2_client = boto3.client('ec2', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

response = ec2_client.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': [instance_name]}])
instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']
print(instance_id, instance_name)

s3_client = boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

response = ec2_client.describe_instances(InstanceIds=[instance_id])
private_ip = response['Reservations'][0]['Instances'][0]['PrivateIpAddress']
print(private_ip)
public_ip = response['Reservations'][0]['Instances'][0]['PublicIpAddress']
print(public_ip)

ssh_username = 'ubuntu'
ssh_password = 'gl*ekofUbr8jo69ufuch'

ssh_client = paramiko.SSHClient()

ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(public_ip, username = ssh_username, password= ssh_password, timeout= 10000.0)

temp_directory = 'ab1/tmp/files'
os.makedirs(temp_directory)

sftp_client = ssh_client.open_sftp()
remote_files = sftp_client.listdir('/home/ubuntu/smrtlink/userdata/jobs_root/0000/0000000/0000000615/outputs/fastx_files/')
print(remote_files)


for file_name in remote_files:
    if file_name.endswith(file_format):
        remote_file_path = f'/home/ubuntu/smrtlink/userdata/jobs_root/0000/0000000/0000000615/outputs/fastx_files/{file_name}'
        print(remote_file_path)
        local_file_path = os.path.join(temp_directory, file_name)
        # print(local_file_path)
        sftp_client.get(remote_file_path, local_file_path)
        s3_key = f'{s3_prefix}/{file_name}' if s3_prefix else file_name
        s3_client.upload_file(local_file_path, bucket_name,s3_key)

sftp_client.close()
ssh_client.close()

os.rmdir(temp_directory)


s3_url = f"https://{bucket_name}.s3-{region}.amazonaws.com/s3_key"
print(f"Data transferred to S3: {s3_url}")
