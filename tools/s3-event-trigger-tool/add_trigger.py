
"""" Notice:
1. put_bucket_notification_configuration S3接口是每次写入都直接替换之前的notification_config，不能新增。
2. 最大100条记录。
3. By default, only the bucket owner can configure notifications on a bucket. However, bucket owners can use a bucket policy to grant permission to other users to set this configuration with s3:PutBucketNotification permission.
""""
from boto3.session import Session


### SETTING ###
Bucket='hzb-s3-to-gcs'
aws_profile_name='s3'  # aws cli profile in local PC
sqs_arn='arn:aws:sqs:us-west-2:278484429967:s3_migration_file_list'
prefix_file_name='prefix_list.txt'
###############

client = Session(profile_name=aws_profile_name).client('s3')

f = open(prefix_file_name)
lines = f.read().splitlines()

# construct prefix list json format
prefix_list_json = []

for prefix in lines:
    prefix_list_json.append({
                'QueueArn': sqs_arn,
                'Events': ['s3:ObjectCreated:*'],
                'Filter': {
                    'Key': {
                        'FilterRules': [
                            {'Name': 'prefix', 'Value': prefix}
                        ]
                    }
                }
            })

# put trigger
response = client.put_bucket_notification_configuration(
    Bucket=Bucket,
    NotificationConfiguration={
        'QueueConfigurations': prefix_list_json,
    },
    SkipDestinationValidation=True
)
print(response)