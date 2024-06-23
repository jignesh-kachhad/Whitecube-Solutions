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