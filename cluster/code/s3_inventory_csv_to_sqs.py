# Send S3 Inventory report CSV file to SQS, simulating S3 trigger SQS messages

import csv
import boto3
import json


# Setting
file_path = "/Users/hzb/Downloads/s3-inventory-report-no-versionid.csv"   # 注意这个文件切分一下
region = "us-west-2"
sqs_queue = "https://sqs.us-west-2.amazonaws.com/278484429967/s3_migration_file_list"
has_versionId = False  # csv文件是否带 versionId
local_debug = True  # 运行在本地电脑，读取~/.aws 中的本地密钥

# Initial sqs client 
if local_debug:  # 运行在本地电脑，读取~/.aws 中的本地密钥
    src_session = boto3.session.Session(profile_name='s3')
    sqs = src_session.client('sqs', region_name=region)
else:  # 运行在EC2上，直接获取服务器带的 IAM Role
    sqs = boto3.client('sqs', region)

# Get csv records
csv_reader = csv.reader(open(file_path))
print("Open", file_path)
job_list = []
for line in csv_reader:
    bucket = line[0]
    key = line[1]
    if has_versionId:
        versionId = line[2]
        size = line[3]
    else:
        size = line[2]
        versionId = 'null'
    
    job_list.append({'Records': [{'s3': {'bucket': {'name': bucket}, 'object': {'key': key, 'size': int(size), 'versionId': versionId}}}]})
print(len(job_list), "jobs in list are ready to send")

# Send messages to SQS, batch=10
sqs_batch = 0
sqs_message = []
for job in job_list:
    # construct sqs messages
    sqs_message.append({
        "Id": str(sqs_batch),
        "MessageBody": json.dumps(job),
    })
    sqs_batch += 1

    # write to sqs in batch 10 or is last one
    if sqs_batch == 10 or job == job_list[-1]:
        try:
            sqs.send_message_batch(QueueUrl=sqs_queue, Entries=sqs_message)
            print("Sent:", sqs_batch, "; Last key:", job['Records'][0]['s3']['object']['key'])
        except Exception as e:
            print(f'Fail to send sqs message: {str(sqs_message)}, {str(e)}')
        sqs_batch = 0
        sqs_message = []

print(f'Complete upload job to queue: {sqs_queue}')

