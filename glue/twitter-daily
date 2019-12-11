import boto3
import json
import sys
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark.sql.functions import *
from etl_utils import dynamo as etl_dynamo, secrets_manager as etl_sm

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Defined global variables
legacy_copy_role = etl_dynamo.get_etl_metadata('legacy_copy_role')
legacy_redshift_url = etl_dynamo.get_etl_metadata('legacy_redshift_url')
creds = etl_sm.get_etl_creds('legacy_etl_password')
current_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
print(current_date)

# Read only specific fields of json file
user_schema = StructType([
            StructField("id_str", StringType(), False), 
            StructField("name", StringType(), False),
            StructField("screen_name", StringType(), False),
            StructField("description", StringType(), False),
            StructField("profile_image_url_https", StringType(), False),
            StructField("favourites_count", LongType(), True), 
            StructField("followers_count", LongType(), True),
            StructField("friends_count", LongType(), True),
            StructField("statuses_count", LongType(), True)
        ])

tweet_schema = StructType([
            StructField("user", StructType([
                    StructField("id_str", StringType(), False),
                    StructField("name", StringType(), False),
                    StructField("screen_name", StringType(), False)
                ]), True),
            StructField("retweeted_status", StructType([
                    StructField("user", StructType([
                        StructField("id_str", StringType(), False),
                        StructField("name", StringType(), False),
                        StructField("screen_name", StringType(), False)
                    ]), True),
                    StructField("favorite_count", LongType(), True)
                ]), True),
            StructField("id_str", StringType(), False),
            StructField("favorite_count", LongType(), True),
            StructField("retweet_count", LongType(), True),
            StructField("created_at", StringType(), True)
])

embed_tweet = StructType([
    StructField("id", StringType(), False),
    StructField("url", StringType(), True),
    StructField("html", StringType(), True)
])

timestampFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"

# Read all users data
path = "s3://aws-glue-temporary-976661725066-us-east-1/twitter_data/" + current_date + "/user_*.json"
# path = "s3://aws-glue-temporary-976661725066-us-east-1/twitter_data/new_data/2019-11-17/user_*.json"
dataframe0 = (spark.read
                    .option("multiline", "true")
                    .schema(user_schema)
                    .json(path))
dataframe0 = dataframe0.replace('Julián Castro', 'Julian Castro', ['name'])
print(dataframe0.printSchema())
print(dataframe0.count())

dataframe0 = dataframe0.withColumn("extracted_at", lit(datetime.now()))
dataframe0 = dataframe0.select(
                            col("id_str").alias("user_id"),
                            col("name").alias("user_name"),
                            col("screen_name"),
                            col("description"),
                            col("profile_image_url_https").alias("profile_image"),
                            col("favourites_count").cast(IntegerType()),
                            col("followers_count").cast(IntegerType()),
                            col("friends_count").cast(IntegerType()),
                            col("statuses_count").cast(IntegerType()),
                            col("extracted_at")
                            )
print(dataframe0.printSchema())
print(dataframe0.count())

# Read all tweets data from users
path = "s3://aws-glue-temporary-976661725066-us-east-1/twitter_data/" + current_date + "/tweets_*.json"
# path = "s3://aws-glue-temporary-976661725066-us-east-1/twitter_data/new_data/2019-11-17/tweets_*.json"
dataframe1 = (spark.read
                    .option("multiline", "true")
                    .schema(tweet_schema)
                    .json(path))
print(dataframe1.cache().printSchema())
print(dataframe1.cache().count())

# Read all tweets data from users
path = "s3://aws-glue-temporary-976661725066-us-east-1/twitter_data/" + current_date + "/embed_tweets_*.json"
# path = "s3://aws-glue-temporary-976661725066-us-east-1/twitter_data/new_data/2019-11-17/embed_tweets_*.json"
dataframe2 = (spark.read
                    .option("multiline", "true")
                    .schema(embed_tweet)
                    .json(path))
dataframe2 = dataframe2.select(
                    col("id").alias("id_str"),
                    col("html"),
                    col("url")
                )
print(dataframe2.cache().printSchema())
print(dataframe2.cache().count())

joined_df = dataframe1.join(dataframe2, 'id_str', 'left_outer')
print(joined_df.printSchema())
print(joined_df.count())

joined_df = joined_df.withColumn("extracted_at", lit(datetime.now()))
joined_df = joined_df.select(col("id_str").alias("tweet_id"),
                            col("user.id_str").alias("user_id"),
                            col("user.name").alias("user_name"),
                            col("user.screen_name").alias("screen_name"),
                            col("retweeted_status.user.id_str").alias("retweet_user_id"),
                            col("retweeted_status.user.name").alias("retweet_user_name"),
                            col("retweeted_status.user.screen_name").alias("retweet_screen_name"),
                            when((joined_df.favorite_count == 0) & (joined_df.retweeted_status.user.id_str != '') , col("retweeted_status.favorite_count")).otherwise(col("favorite_count")).alias("favorite_count"),
                            col("retweet_count").alias("retweet_count"),
                            col("html"),
                            col("url"),
                            unix_timestamp("created_at", timestampFormat).cast(TimestampType()).alias("created_at"),
                            col("extracted_at")
                        )
print(joined_df.printSchema())
print(joined_df.cache().count())

# (dataframe0.write
#         .format("com.databricks.spark.redshift")
#         .option("url", legacy_redshift_url)
#         .option("user", "etl")
#         .option("password", creds)
#         .option("dbtable", "dems2020.user")
#         .option("aws_iam_role", legacy_copy_role)
#         .option("tempdir", args["TempDir"])
#         .mode("append")
#         .save()
# )

# (joined_df.write
#         .format("com.databricks.spark.redshift")
#         .option("url", legacy_redshift_url)
#         .option("user", "etl")
#         .option("password", creds)
#         .option("dbtable", "dems2020.tweet")
#         .option("aws_iam_role", legacy_copy_role)
#         .option("tempdir", args["TempDir"])
#         .mode("append")
#         .save()
# )

job.commit()
