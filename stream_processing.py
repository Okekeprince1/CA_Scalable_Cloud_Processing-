import json
import csv
import re
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, udf, explode, split, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import boto3

# Initialize Spark session
spark = SparkSession.builder.appName("YelpReviewStreaming").getOrCreate()

# Define schema
schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("stars", DoubleType(), True),
    StructField("useful", IntegerType(), True),
    StructField("funny", IntegerType(), True),
    StructField("cool", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("date", StringType(), True)
])

folder_prefix = "1GB"

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
table = dynamodb.Table('ReviewAnalysisStream')
bucket = 'scalableyelpdata'

# Keyword extraction UDF
def extract_keywords(text):
    if text is None:
        return ""
    # extract words using regex
    tokens = re.findall(r'\w+', text.lower())
    return ",".join(tokens)
keyword_udf = udf(extract_keywords, StringType())

# Sentiment keywords UDF
good_keywords = {'good', 'great', 'amazing', 'excellent', 'delicious', 'wonderful', 'awesome'}
bad_keywords = {'bad', 'poor', 'slow', 'terrible', 'awful'}
def classify_review(text):
    text_lower = text.lower()
    has_good = any(kw in text_lower for kw in good_keywords)
    has_bad = any(kw in text_lower for kw in bad_keywords)
    if has_good and not has_bad:
        return 'good'
    elif has_bad and not has_good:
        return 'bad'
    return 'neutral'
classify_udf = udf(classify_review, StringType())

# Logging function
def log_metrics(start_time, record_count, analysis_type):
    latency = time.time() - start_time
    throughput = record_count / latency if latency > 0 else 0
    print(f"[{analysis_type}] Processed {record_count} records, Latency: {latency:.2f}s, Throughput: {throughput:.2f} records/s")
    return latency, throughput

# Read from Kinesis
endpointUrl = "https://kinesis.us-east-1.amazonaws.com"
kinesis = spark \
    .readStream \
    .format("aws-kinesis") \
    .option("kinesis.region", "us-east-1") \
    .option("kinesis.streamName", "CA-Scalable-Kinesis-Stream") \
    .option("kinesis.consumerType", "GetRecords") \
    .option("kinesis.endpointUrl", endpointUrl) \
    .option("kinesis.startingPosition", "TRIM_HORIZON") \
    .load()

# Parse JSON data
reviews = kinesis.selectExpr("CAST(data AS STRING) AS json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Process each batch
def process_batch(df, batch_id):
    start_time = time.time()
    record_count = df.count()
    if record_count == 0:
        return

    # 1. Trending Keywords (5-minute window)
    start_time_keywords = time.time()
    df_with_keywords = df.withColumn('keywords', keyword_udf(col('text')))
    keywords_exploded = df_with_keywords.selectExpr('business_id', 'explode(split(keywords, ",")) as keyword', 'date as timestamp')
    trending_keywords = keywords_exploded.groupBy(
        window(col('timestamp'), '5 minutes'), 'keyword'
    ).count().orderBy(col('count').desc()).limit(5)
    keyword_list = trending_keywords.collect()
    log_metrics(start_time_keywords, record_count, f'Batch {batch_id} Trending Keywords')
    for row in keyword_list:
        table.put_item(Item={
            'analysis_type': 'trending_keywords',
            'key': f"{row['window'].start}_{row['keyword']}",
            'value': str(row['count'])
        })
    flattened = trending_keywords.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("keyword"),
    col("count")
)
    flattened.write.mode('append').csv(f's3://{bucket}/output/{folder_prefix}/trending_keywords/batch_{batch_id}')

    # 2. Real-Time Rating Updates
    start_time_ratings = time.time()
    avg_ratings = df.groupBy('business_id').agg({'stars': 'avg'}).withColumnRenamed('avg(stars)', 'avg_rating')
    ratings_list = avg_ratings.collect()
    log_metrics(start_time_ratings, record_count, f'Batch {batch_id} Real-Time Ratings')
    for row in ratings_list:
        table.put_item(Item={
            'analysis_type': 'realtime_ratings',
            'key': row['business_id'],
            'value': str(row['avg_rating'])
        })
    avg_ratings.write.mode('append').csv(f's3://{bucket}/output/{folder_prefix}/realtime_ratings/batch_{batch_id}')

    # 3. Count of Good and Bad Reviews
    start_time_sentiment = time.time()
    df_with_sentiment = df.withColumn('sentiment', classify_udf(col('text')))
    sentiment_counts = df_with_sentiment.groupBy('business_id', 'sentiment').count()
    sentiment_list = sentiment_counts.collect()
    log_metrics(start_time_sentiment, record_count, f'Batch {batch_id} Sentiment Counts')
    for row in sentiment_list:
        table.put_item(Item={
            'analysis_type': 'sentiment_counts',
            'key': f"{row['business_id']}_{row['sentiment']}",
            'value': str(row['count'])
        })
    sentiment_counts.write.mode('append').csv(f's3://{bucket}/output/{folder_prefix}/sentiment_counts/batch_{batch_id}')

    log_metrics(start_time, record_count, f'Batch {batch_id} Total')

# Write stream to console and process batches
query = reviews \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", f"s3://{bucket}/checkpoints/") \
    .start()

# Collect and write streaming metrics to CSV
with open("streaming_metrics.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "inputRowsPerSecond", "processedRowsPerSecond", "triggerExecutionMs"])
    
    while query.isActive:
        progress = query.lastProgress
        if progress:
            writer.writerow([
                time.time(),
                progress.get("inputRowsPerSecond", 0),
                progress.get("processedRowsPerSecond", 0),
                progress.get("durationMs", {}).get("triggerExecution", 0)
            ])
        time.sleep(10)


query.awaitTermination()