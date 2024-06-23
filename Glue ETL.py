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
