
import boto3
import paramiko
import os

region = 'us-east-1'

aws_access_key_id = 'AKIA52UT325RXFZOC45A'
aws_secret_access_key = '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'

instance_name = 'test'

bucket_name = 'abdata'

ec2_client = boto3.client('ec2', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

response = ec2_client.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': [instance_name]}])
instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']

s3_client = boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

response = ec2_client.describe_instances(InstanceIds=[instance_id])
private_ip = response['Reservations'][0]['Instances'][0]['PrivateIpAddress']

os.system('tar -czvf mahesh1.txt /path/to/data')

s3_client.upload_file('mahesh1.txt', bucket_name, 'mahesh1.txt')

s3_url = f"https://{bucket_name}.s3-{region}.amazonaws.com/mahesh1.txt"
print(f"Data transferred to S3: {s3_url}")