# Whitecube-Solutions-Assignment
<details>
<summary>Overview</summary>

I have designed and implemented an automated data pipeline using AWS services, specifically leveraging EventBridge Rules and Step Functions to meet all project requirements.

1. **Data Ingestion and Storage**:
    - The pipeline incrementally reads data (both INSERTED and MODIFIED records) from two DynamoDB tables: `user_Campaign` and `campaignInfo`.
    - This data is ingested through a Lambda function, which processes the records and stores the raw data in an S3 bucket.

2. **Data Transformation**:
    - The raw data stored in S3 serves as the source for AWS Glue ETL jobs.
    - Within AWS Glue, the `user_Campaign` data is flattened, and a new column `taskDeadline` is added by performing a join operation with the `campaignInfo` table based on `campaignID` and `taskID`.
    - Necessary schema modifications are applied to ensure data consistency and integrity.
    - The data is then partitioned using the `createdAt` column, facilitating efficient querying and storage.
    - The transformed data is saved back to an S3 location.

3. **Data Crawling and Querying**:
    - An AWS Glue Crawler is used to catalog the transformed data stored in S3.
    - The cataloged data is then made available for querying in Amazon Athena.

4. **Notification System**:
    - An SNS (Simple Notification Service) is integrated to monitor the ETL process.
    - On completion of the ETL job, whether it succeeds or fails, an email notification is sent to inform me of the outcome.
    - Additionally, when new data is ready for querying in Athena, a final notification email is sent to indicate that the data is available.

This setup ensures a robust, automated, and scalable data pipeline that efficiently handles data ingestion, transformation, and querying, while also providing real-time notifications on the process.

</details>

![Whitecube Solutions.png](https://github.com/jignesh-kachhad/Whitecube-Solutions/blob/main/Architecture.png)

<details>
<summary>Code: Create DynamoDB table and Enable DynamoDB stream using Python Script</summary>

```python
import boto3
from botocore.exceptions import ClientError

# Initialize a session using Amazon DynamoDB
dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")

# Create the user_Campaign table
user_campaign_table = dynamodb.create_table(
    TableName="user_Campaign",
    KeySchema=[
        {"AttributeName": "userID", "KeyType": "HASH"}, 
        {"AttributeName": "campaignID", "KeyType": "RANGE"},
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
    KeySchema=[{"AttributeName": "campaignID", "KeyType": "HASH"}],  
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
```
</details>


<details>
<summary>Code: Ingest Data In DynamoDB Tables using Python Script</summary>

```python
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
```
</details>

<details>
<summary>Lambda Code: Capture Inserted and Modified Raw Data from DynamoDB and Store it into S3</summary>

```python
import json
import boto3

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket_name = "updated-data-from-dynamodb"

    for record in event["Records"]:
        if record["eventName"] == "INSERT" or record["eventName"] == "MODIFY":
            # Extract table name from event record
            table_name = record["eventSourceARN"].split(":")[5].split("/")[1]
            
            if table_name == "user_Campaign":
                process_user_campaign(record, bucket_name)
            elif table_name == "campaignInfo":
                process_campaign_info(record, bucket_name)

    return {"statusCode": 200, "body": json.dumps("Records processed successfully")}

def process_user_campaign(record, bucket_name):
    new_image = record["dynamodb"]["NewImage"]
    user_id = new_image["userID"]["S"]
    created_at = new_image["createdAt"]["S"]
    
    # Transform and write the data to S3
    transformed_record = {
        "userID": new_image["userID"]["S"],
        "campaignID": new_image["campaignID"]["S"],
        "userCampaignProgressState": new_image["userCampaignProgressState"]["S"],
        "createdAt": new_image["createdAt"]["S"],
        "updatedAt": new_image["updatedAt"]["S"],
        "totalTaskRewardsEarned": int(new_image["totalTaskRewardsEarned"]["N"]),
        "progressDetails": [
            {
                "taskID": task["M"]["taskID"]["S"],
                "userCampaignTaskProgressState": task["M"]["userCampaignTaskProgressState"]["S"],
                "validatedAt": task["M"]["validatedAt"]["S"],
                "validationFailureReason": task["M"]["validationFailureReason"]["S"],
                "rewardEarned": int(task["M"]["rewardEarned"]["N"]),
            }
            for task in new_image["progressDetails"]["L"]
        ],
    }

    s3.put_object(
        Bucket=bucket_name,
        Key=f"raw_data/user_campaign/{user_id}_{created_at}.json",
        Body=json.dumps(transformed_record),
    )

def process_campaign_info(record, bucket_name):
    new_image = record["dynamodb"]["NewImage"]
    campaign_id = new_image["campaignID"]["S"]
    
    # Transform and write the data to S3
    transformed_record = {
        "campaignID": new_image["campaignID"]["S"],
        "title": new_image["title"]["S"],
        "rewardClaimDeadline": new_image["rewardClaimDeadline"]["S"],
        "endsAtTimestamp": new_image["endsAtTimestamp"]["S"],
        "createdAt": new_image["createdAt"]["S"],
        "updatedAt": new_image["updatedAt"]["S"],
        "campaignTasks": [
            {
                "taskID": task["M"]["taskID"]["S"],
                "rewardAmount": int(task["M"]["rewardAmount"]["N"]),
                "taskDeadline": task["M"]["taskDeadline"]["S"],
            }
            for task in new_image["campaignTasks"]["L"]
        ],
    }

    s3.put_object(
        Bucket=bucket_name,
        Key=f"raw_data/campaign_info/{campaign_id}.json",
        Body=json.dumps(transformed_record),
    )
```
</details>


## Automating the whole pipeline using the Step function

![State machine](https://github.com/jignesh-kachhad/Whitecube-Solutions/blob/main/State%20Machine.png)

![SNS](https://github.com/jignesh-kachhad/Whitecube-Solutions/blob/main/SNS%20Email%20from%20AWS.png)

<details>
<summary>Code: State Machine</summary>

```json
{
  "Comment": "A description of my state machine",
  "StartAt": "Start_raw_data_Crawler",
  "States": {
    "Start_raw_data_Crawler": {
      "Type": "Task",
      "Parameters": {
        "Name": "crawl_raw_data"
      },
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Next": "GetCrawler"
    },
    "GetCrawler": {
      "Type": "Task",
      "Parameters": {
        "Name": "crawl_raw_data"
      },
      "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
      "Next": "Is_Running?"
    },
    "Is_Running?": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.Crawler.State",
          "StringMatches": "RUNNING",
          "Next": "Wait for 5 Sec"
        }
      ],
      "Default": "Glue StartJobRun"
    },
    "Wait for 5 Sec": {
      "Type": "Wait",
      "Seconds": 5,
      "Next": "GetCrawler"
    },
    "Glue StartJobRun": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "Transformation"
      },
      "Next": "Glue_Job_Status",
      "Catch": [
        {
          "ErrorEquals": [
            "States.TaskFailed"
          ],
          "Next": "Failed_Notification"
        }
      ]
    },
    "Glue_Job_Status": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.JobRunState",
          "StringMatches": "SUCCEEDED",
          "Next": "Success_Notification"
        }
      ],
      "Default": "Failed_Notification"
    },
    "Success_Notification": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:ap-south-1:126362963275:s3-arrival-notification",
        "Message": "Glue job Execution Successful !!"
      },
      "Next": "Start_Transformed_data_Crawler"
    },
    "Start_Transformed_data_Crawler": {
      "Type": "Task",
      "Parameters": {
        "Name": "Crawl_transformed_data"
      },
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Next": "GetCrawler (1)"
    },
    "GetCrawler (1)": {
      "Type": "Task",
      "Parameters": {
        "Name": "Crawl_transformed_data"
      },
      "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
      "Next": "is_running?"
    },
    "is_running?": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.Crawler.State",
          "StringMatches": "RUNNING",
          "Next": "Wait"
        }
      ],
      "Default": "Final Notification"
    },
    "Final Notification": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:ap-south-1:126362963275:s3-arrival-notification",
        "Message": "All processes are complete. You can now start querying in Athena."
      },
      "End": true
    },
    "Wait": {
      "Type": "Wait",
      "Seconds": 5,
      "Next": "GetCrawler (1)"
    },
    "Failed_Notification": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "Message.$": "$",
        "TopicArn": "arn:aws:sns:ap-south-1:126362963275:s3-arrival-notification"
      },
      "End": true
    }
  }
}
```
</details>

## **Glue ETL: Flatten, Join, Partitioned and Store Transformed Data in S3**

![Glue ETL](https://github.com/jignesh-kachhad/Whitecube-Solutions/blob/main/Glue%20ETL.png)

<details>
<summary>Code: Glue ETL</summary>

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_flatten
import gs_explode

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node raw_dynamodb_user_campaign_data
raw_dynamodb_user_campaign_data_node1719137368433 = glueContext.create_dynamic_frame.from_catalog(
    database="whitecube_raw_data", 
    table_name="user_campaign", 
    transformation_ctx="raw_dynamodb_user_campaign_data_node1719137368433"
)

# Script generated for node raw_dynamodb_campaign_Info_data
raw_dynamodb_campaign_Info_data_node1719142294763 = glueContext.create_dynamic_frame.from_catalog(
    database="whitecube_raw_data", 
    table_name="campaign_info", 
    transformation_ctx="raw_dynamodb_campaign_Info_data_node1719142294763"
)

# Script generated for node Explode Array Or Map Into Rows
ExplodeArrayOrMapIntoRows_node1719140360403 = raw_dynamodb_user_campaign_data_node1719137368433.gs_explode(
    colName="progressdetails", 
    newCol="progressDetails"
)

# Script generated for node Explode Array Or Map Into Rows
ExplodeArrayOrMapIntoRows_node1719142383397 = raw_dynamodb_campaign_Info_data_node1719142294763.gs_explode(
    colName="campaigntasks", 
    newCol="campaigntask"
)

# Script generated for node Flatten_user_campaign
Flatten_user_campaign_node1719140478897 = ExplodeArrayOrMapIntoRows_node1719140360403.gs_flatten()

# Script generated for node Flatten_campaign_Info
Flatten_campaign_Info_node1719144438018 = ExplodeArrayOrMapIntoRows_node1719142383397.gs_flatten()

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1719144574758 = ApplyMapping.apply(
    frame=Flatten_campaign_Info_node1719144438018, 
    mappings=[
        ("campaignID", "string", "right_campaignID", "string"), 
        ("title", "string", "right_title", "string"), 
        ("rewardClaimDeadline", "string", "right_rewardClaimDeadline", "string"), 
        ("endsAtTimestamp", "string", "right_endsAtTimestamp", "string"), 
        ("createdAt", "string", "right_createdAt", "string"), 
        ("updatedAt", "string", "right_updatedAt", "string"), 
        ("campaignTasks", "array", "right_campaignTasks", "array"), 
        ("`campaigntask.taskID`", "string", "`right_campaigntask.taskID`", "string"), 
        ("`campaigntask.rewardAmount`", "int", "`right_campaigntask.rewardAmount`", "int"), 
        ("`campaigntask.taskDeadline`", "string", "`right_campaigntask.taskDeadline`", "string")
    ], 
    transformation_ctx="RenamedkeysforJoin_node1719144574758"
)

# Script generated for node Join
Join_node1719144519563 = Join.apply(
    frame1=Flatten_user_campaign_node1719140478897, 
    frame2=RenamedkeysforJoin_node1719144574758, 
    keys1=["campaignID", "`progressDetails.taskID`"], 
    keys2=["right_campaignID", "`right_campaigntask.taskID`"], 
    transformation_ctx="Join_node1719144519563"
)

# Script generated for node Change Schema
ChangeSchema_node1719144619084 = ApplyMapping.apply(
    frame=Join_node1719144519563, 
    mappings=[
        ("userID", "string", "userID", "string"), 
        ("campaignID", "string", "campaignID", "string"), 
        ("userCampaignProgressState", "string", "userCampaignProgressState", "string"), 
        ("createdAt", "string", "createdAt", "string"), 
        ("updatedAt", "string", "updatedAt", "string"), 
        ("totalTaskRewardsEarned", "int", "totalTaskRewardsEarned", "int"), 
        ("`progressDetails.taskID`", "string", "taskID", "string"), 
        ("`progressDetails.userCampaignTaskProgressState`", "string", "userCampaignTaskProgressState", "string"), 
        ("`progressDetails.validatedAt`", "string", "validatedAt", "string"), 
        ("`progressDetails.validationFailureReason`", "string", "validationFailureReason", "string"), 
        ("`progressDetails.rewardEarned`", "int", "rewardEarned", "int"), 
        ("`right_campaigntask.taskDeadline`", "string", "taskDeadline", "string")
    ], 
    transformation_ctx="ChangeSchema_node1719144619084"
)

# Script generated for node Target S3
TargetS3_node1719144875681 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1719144619084, 
    connection_type="s3", 
    format="csv", 
    connection_options={
        "path": "s3://campaign-transformed-data", 
        "partitionKeys": ["createdAt"]
    }, 
    transformation_ctx="TargetS3_node1719144875681"
)

job.commit()

```
</details>
