import boto3
from botocore.exceptions import ClientError

# Initialize a session using Amazon DynamoDB
dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")

# Create the user_Campaign table
user_campaign_table = dynamodb.create_table(
    TableName="user_Campaign",
    KeySchema=[
        {"AttributeName": "userID", "KeyType": "HASH"},  # Partition key
        {"AttributeName": "campaignID", "KeyType": "RANGE"},  # Sort key
    ],
    AttributeDefinitions=[
        {"AttributeName": "userID", "AttributeType": "S"},
        {"AttributeName": "campaignID", "AttributeType": "S"},
    ],
    ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
)

# Create the campaignInfo table
campaign_info_table = dynamodb.create_table(
    TableName="campaignInfo",
    KeySchema=[{"AttributeName": "campaignID", "KeyType": "HASH"}],  # Partition key
    AttributeDefinitions=[{"AttributeName": "campaignID", "AttributeType": "S"}],
    ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
)

# Wait until the tables exist
user_campaign_table.meta.client.get_waiter("table_exists").wait(
    TableName="user_Campaign"
)
campaign_info_table.meta.client.get_waiter("table_exists").wait(
    TableName="campaignInfo"
)

print("Tables created successfully.")

dynamodb = boto3.client("dynamodb")

dynamodb.update_table(
    TableName="user_Campaign",
    StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
)

dynamodb.update_table(
    TableName="campaignInfo",
    StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
)

print("Streams enabled successfully.")
