"""
类似 CLI
aws s3api get-bucket-notification-configuration --bucket hzb-s3-to-gcs --profile s3
"""

from boto3.session import Session


### SETTING ###
Bucket='hzb-cross-owner2'
aws_profile_name='s3'  # aws cli profile in local PC
prefix_file_name='prefix_list_output.txt'
###############

client = Session(profile_name=aws_profile_name).client('s3')

# list trigger
response = client.get_bucket_notification_configuration(Bucket=Bucket)
prefix_list=[]
for n in response['QueueConfigurations']:
    preid = n['Id']
    prefix = n['Filter']['Key']['FilterRules'][0]['Value']
    prefix_list.append([preid, prefix])

for p in prefix_list:
    print(p[0], p[1])
