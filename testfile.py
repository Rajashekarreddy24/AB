import boto3
import paramiko

aws_access_key_id = 'AKIA52UT325RXFZOC45A'
aws_secret_access_key = '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'
instance_name = 'test'
region = 'us-east-1'
bucket_name = 'abdata'
ec2_client = boto3.client('ec2',aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key,region_name = region )
s3_client = boto3.client('s3', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name= region)
file_format = '.txt'

ssh_username = 'ubuntu'
ssh_password = ' Test@123'

response = ec2_client.describe_instances(Filters = [{'Name': 'tag:Name','Values' :[instance_name]}])
instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']

response = ec2_client.describe_instances(InstanceIds = [instance_id])
private_id = response['Reservations'][0]['Instances'][0]['PrivateIpAddress']
print(private_id)



ssh_client = paramiko.SSHClient()
ssh_client.connect(private_id,username= ssh_username,password= ssh_password)

sftp_client = ssh_client.open_sftp()
remote_files = sftp_client.listdir('/path/to/remote/directory')
print(remote_files)


for file_name in remote_files:
    if file_name.endswith(file_format):
        remote_file_path = f'/path/to/remote/directory/{file_name}'
        sftp_client.get(remote_file_path)
        
sftp_client.close()
ssh_client.close()

s3_client.upload_file(remote_file_path,bucket_name,'files.gz')

s3_url = f"https://{bucket_name}.s3-{region}.amazonaws.com/files.tar.gz"
print(f"Data transferred to S3: {s3_url}")

