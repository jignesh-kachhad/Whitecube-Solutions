import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")

# Sample Data
user_campaign_data = [
    {
        "userID": "user-002",
        "campaignID": "101",
        "userCampaignProgressState": "INPROGRESS",
        "createdAt": "2024-01-01T00:00:00Z",
        "updatedAt": "2024-01-19T00:00:00Z",
        "totalTaskRewardsEarned": 20,
        "progressDetails": [
            {
                "taskID": "task002",
                "userCampaignTaskProgressState": "VALIDATED",
                "validatedAt": "2024-01-10T00:00:00Z",
                "validationFailureReason": "",
                "rewardEarned": 20,
            },
            {
                "taskID": "task005",
                "userCampaignTaskProgressState": "VALIDATED",
                "validatedAt": "2024-05-10T00:00:00Z",
                "validationFailureReason": "",
                "rewardEarned": 30,
            },
        ],
    }
]

campaign_info_data = [
    {
        "campaignID": "101",
        "title": "New Year Airdrop 2024",
        "rewardClaimDeadline": "2024-12-31T23:59:59Z",
        "endsAtTimestamp": "2024-01-31T23:59:59Z",
        "createdAt": "2023-12-01T00:00:00Z",
        "updatedAt": "2023-12-05T00:00:00Z",
        "campaignTasks": [
            {
                "taskID": "task002",
                "rewardAmount": 20,
                "taskDeadline": "2024-01-20T23:59:59Z",
            },
            {
                "taskID": "task005",
                "rewardAmount": 10,
                "taskDeadline": "2024-05-20T23:59:59Z",
            },
        ],
    }
]

user_campaign_table = dynamodb.Table("user_Campaign")
campaign_info_table = dynamodb.Table("campaignInfo")

# Function to write a single item to DynamoDB
def write_to_dynamodb(table, item):
    try:
        table.put_item(Item=item)
    except ClientError as e:
        print(e.response["Error"]["Message"])


# Write each user_campaign record to DynamoDB
for item in user_campaign_data:
    write_to_dynamodb(user_campaign_table, item)

# Write each campaign_info record to DynamoDB
for item in campaign_info_data:
    write_to_dynamodb(campaign_info_table, item)

print("Data inserted successfully.")
