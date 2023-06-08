
import boto3
import paramiko
import tarfile
import os

region = 'us-east-1'

aws_access_key_id = 'AKIA52UT325RXFZOC45A'
aws_secret_access_key = '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'

instance_name = 'test'

bucket_name = 'abdata'

file_format = '.txt'

ec2_client = boto3.client('ec2', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

response = ec2_client.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': [instance_name]}])
instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']
print(instance_id, instance_name)

s3_client = boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

response = ec2_client.describe_instances(InstanceIds=[instance_id])
private_ip = response['Reservations'][0]['Instances'][0]['PrivateIpAddress']
print(private_ip)

ssh_username = 'ubuntu'
# ssh_private_key_path = r'C:\Users\prajashekar\Downloads\test.pem'
ssh_password = 'Test@123'

ssh_client = paramiko.SSHClient()
# ssh_password = paramiko.PasswordRequiredException (ssh_password)
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# # ssh_key = paramiko.RSAKey.from_private_key_file(ssh_private_key_path)
ssh_client.connect(private_ip)

temp_directory = 'ab/tmp/files'
os.makedirs(temp_directory)

sftp_client = ssh_client.open_sftp()
remote_files = sftp_client.listdir('/path/to/remote/directory')

for file_name in remote_files:
    if file_name.endswith(file_format):
        remote_file_path = f'/path/to/remote/directory/{file_name}'
        local_file_path = os.path.join(temp_directory, file_name)
        sftp_client.get(remote_file_path, local_file_path)

sftp_client.close()
ssh_client.close()

compressed_file_path = '/tmp/files.tar.gz'
with tarfile.open(compressed_file_path, 'w:gz') as tar:
    tar.add(temp_directory, arcname=os.path.basename(temp_directory))

s3_client.upload_file(compressed_file_path, bucket_name, 'files.tar.gz')

os.remove(compressed_file_path)
os.rmdir(temp_directory)


s3_url = f"https://{bucket_name}.s3-{region}.amazonaws.com/files.tar.gz"
print(f"Data transferred to S3: {s3_url}")
