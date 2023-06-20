
import boto3
import paramiko
import os
from pathlib import Path


region = 'us-east-1'
aws_access_key_id = 'AKIA52UT325RXFZOC45A'
aws_secret_access_key = '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'

instance_name = 'test'

bucket_name = 'abdata'

# source_path = '/home/ubuntu/smrtlink/userdata/jobs_root/0000/0000000/0000000615/outputs/fastx_files/'
# splits= Path(source_path).parts

# folder_name = f'0000/0000000/{splits[8]}/{splits[9]}/{splits[10]}'
# # '0000/0000000/0000000615/outputs/fastx_files'
ec2_client = boto3.client('ec2', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

response = ec2_client.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': [instance_name]}])
instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']

print(instance_id)

response = ec2_client.describe_instances(InstanceIds=[instance_id])
private_ip = response['Reservations'][0]['Instances'][0]['PrivateIpAddress']
public_ip = response['Reservations'][0]['Instances'][0]['PublicIpAddress']

ssh_username = 'ubuntu'
ssh_password = 'Test@123'

ssh_client = paramiko.SSHClient()

ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(public_ip, username = ssh_username, password= ssh_password, timeout= 10000.0)


stdin, stdout, stedrr = ssh_client.exec_command('pwd')

output = stdout.read().decode().split()
print(output)
# for file_path in output:
#     print(file_path)