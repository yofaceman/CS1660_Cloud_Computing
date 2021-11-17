import boto3

dyndb = boto3.resource('dynamodb',
                      region_name = 'us-west-2',
                      aws_access_key_id='AKIA6M7NPIJCC575CX65',
                      aws_secret_access_key='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')

table = dyndb.Table("DataTable")

pKey = input("Enter Partition Key:")
rKey = input("Enter Row Key:")

response = table.get_item(
    Key={
        'PartitionKey': pKey,
        'RowKey': rKey
    }
)
item = response['Item']
print(item)
