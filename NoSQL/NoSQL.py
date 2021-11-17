import boto3
import csv

s3 = boto3.resource('s3',
                    aws_access_key_id='AKIA6M7NPIJCC575CX65',
                    aws_secret_access_key='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')

try:
    s3.create_bucket(Bucket='datacont-matthew-arndt', CreateBucketConfiguration={
        'LocationConstraint': 'us-west-2'})
except Exception as e:
    print(e)

bucket = s3.Bucket("datacont-matthew-arndt")
bucket.Acl().put(ACL='public-read')

body = open(r'C:\Users\Matthew\OneDrive\Documents\GitHub\CS1660_Cloud_Computing\NoSQL\experiments.csv','rb')
o = s3.Object('datacont-matthew-arndt', 'test').put(Body=body)
s3.Object('datacont-matthew-arndt','test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
                      region_name = 'us-west-2',
                      aws_access_key_id='AKIA6M7NPIJCC575CX65',
                      aws_secret_access_key='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')

try:
    table = dyndb.create_table(
        TableName = 'DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except Exception as e:
    print(e)

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

with open(r'C:\Users\Matthew\OneDrive\Documents\GitHub\CS1660_Cloud_Computing\NoSQL\experiments.csv', 'rt') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(csvf)
    for item in csvf:
        print(item)
        body = open(r'C:\Users\Matthew\OneDrive\Documents\GitHub\CS1660_Cloud_Computing\NoSQL\\'+item[4], 'rb')
        s3.Object('datacont-matthew-arndt', item[4]).put(Body=body)
        md = s3.Object('datacont-matthew-arndt', item[4]).Acl().put(ACL='public-read')

        url = " https://s3-us-west-2.amazonaws.com/datacont-matthew-arndt/"+item[4]
        metadata_item = {
            'PartitionKey': item[4],
            'RowKey': item[0],
            'Conductivity': item[2],
            'Temp': item[1],
            'Concentration': item[3],
            'url': url
        }
        try:
            table.put_item(Item=metadata_item)
        except Exception as e:
            print("Item may already be there or another failure")

response = table.get_item(
    Key={
        'PartitionKey': 'exp3.csv',
        'RowKey': '3'
    }
)
item = response['Item']
print(item)

response
