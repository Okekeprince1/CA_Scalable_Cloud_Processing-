import json
import re
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import boto3
import nltk

# Initialize NLTK resources
nltk.download('vader_lexicon')
nltk.download('punkt')
nltk.download('stopwords')

# Initialize Spark session
spark = SparkSession.builder.appName("YelpReviewMapReduce").getOrCreate()

# Define schema for review data
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

# AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ReviewAnalysis1GB')
bucket = 'scalableyelpdata'

# Sentiment analysis UDF
sid = SentimentIntensityAnalyzer()
def get_sentiment(text):
    if text is None or not isinstance(text, str):
        return 0.0
    scores = sid.polarity_scores(text)
    return scores['compound']
sentiment_udf = udf(get_sentiment, DoubleType())

# Keyword extraction UDF
stop_words = set(stopwords.words('english'))
def extract_keywords(text):
    if text is None or not isinstance(text, str):
        return ''
    # Tokenize using regex: split on non-alphanumeric characters
    tokens = re.findall(r'\b\w+\b', text.lower())
    keywords = [word for word in tokens if word.isalnum() and word not in stop_words]
    return ','.join(keywords)
keyword_udf = udf(extract_keywords, StringType())

# Logging function
def log_metrics(start_time, record_count, analysis_type):
    latency = time.time() - start_time
    throughput = record_count / latency if latency > 0 else 0
    print(f"[{analysis_type}] Processed {record_count} records, Latency: {latency:.2f}s, Throughput: {throughput:.2f} records/s")
    return latency, throughput

# Load data from S3
start_time = time.time()
df = spark.read.schema(schema).json('s3://scalableyelpdata/inputs/dataset_5000MB.json')
record_count = df.count()

# 1. Average Rating per Business
start_time_avg = time.time()
avg_ratings = df.groupBy('business_id').agg({'stars': 'avg'}).withColumnRenamed('avg(stars)', 'avg_rating')
avg_ratings_list = avg_ratings.collect()
log_metrics(start_time_avg, record_count, 'Average Rating')

for row in avg_ratings_list:
    table.put_item(Item={'analysis_type': 'avg_rating', 'key': row['business_id'], 'value': str(row['avg_rating'])})

avg_ratings.write.mode('overwrite').csv('s3://scalableyelpdata/output/avg_ratings')

# 2. Sentiment Analysis
start_time_sent = time.time()
df_with_sentiment = df.withColumn('sentiment', sentiment_udf(col('text')))
avg_sentiment = df_with_sentiment.groupBy('business_id').agg({'sentiment': 'avg'}).withColumnRenamed('avg(sentiment)', 'avg_sentiment')
sentiment_list = avg_sentiment.collect()
log_metrics(start_time_sent, record_count, 'Sentiment Analysis')

for row in sentiment_list:
    table.put_item(Item={'analysis_type': 'sentiment', 'key': row['business_id'], 'value': str(row['avg_sentiment'])})

avg_sentiment.write.mode('overwrite').csv('s3://scalableyelpdata/output/sentiment')

# 3. Most Common Complaints or Praises
start_time_keywords = time.time()
df_with_keywords = df.withColumn('keywords', keyword_udf(col('text')))
keywords_exploded = df_with_keywords.selectExpr('business_id', 'explode(split(keywords, ",")) as keyword')
keyword_counts = keywords_exploded.groupBy('keyword').count().orderBy(col('count').desc()).limit(10)
keyword_list = keyword_counts.collect()
log_metrics(start_time_keywords, record_count, 'Keyword Analysis')

for row in keyword_list:
    table.put_item(Item={'analysis_type': 'keywords', 'key': row['keyword'], 'value': str(row['count'])})

keyword_counts.write.mode('overwrite').csv('s3://scalableyelpdata/output/keywords')

# 4. Temporal Trends in Ratings
start_time_trends = time.time()
df_with_year = df.withColumn('year', year(col('date')))
temporal_trends = df_with_year.groupBy('business_id', 'year').agg({'stars': 'avg'}).withColumnRenamed('avg(stars)', 'avg_rating')
trends_list = temporal_trends.collect()
log_metrics(start_time_trends, record_count, 'Temporal Trends')

for row in trends_list:
    table.put_item(Item={'analysis_type': 'temporal_trends', 'key': f"{row['business_id']}_{row['year']}", 'value': str(row['avg_rating'])})

temporal_trends.write.mode('overwrite').csv('s3://scalableyelpdata/output/temporal_trends')

# Final log
total_latency, total_throughput = log_metrics(start_time, record_count, 'MapReduce Total')
spark.stop()