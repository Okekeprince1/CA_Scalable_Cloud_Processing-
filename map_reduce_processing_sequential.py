import json
import time
import re
import boto3
import pandas as pd
from collections import defaultdict, Counter
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords


s3_client = boto3.client('s3')
bucket = 'scalableyelpdata'
key = 'inputs/dataset_3072MB.json'


sid = SentimentIntensityAnalyzer()

stop_words = set(stopwords.words('english'))

def extract_keywords(text):
    if text is None or not isinstance(text, str):
        return []
    tokens = re.findall(r'\b\w+\b', text.lower())
    keywords = [w for w in tokens if w not in stop_words and w.isalnum()]
    return keywords

# ---------- Benchmarking ----------

def log_metrics(start_time, processed_count, analysis_type, processing_type='sequential'):
    latency = time.time() - start_time
    throughput = processed_count / latency if latency > 0 else 0
    metrics = {
        'analysis_type': f"{processing_type}_{analysis_type}",
        'processed_count': processed_count,
        'latency': latency,
        'throughput': throughput
    }
    print(f"[{processing_type}][{analysis_type}] Processed {processed_count} records, "
          f"Latency: {latency:.2f}s, Throughput: {throughput:.2f} records/s")
    return metrics


print(f"Loading JSON file from s3://{bucket}/{key}")

start_total = time.time()
metrics_list = []

# Running sums/counts
ratings_sum = defaultdict(float)
ratings_count = defaultdict(int)
sentiment_sum = defaultdict(float)
sentiment_count = defaultdict(int)
temporal_ratings = defaultdict(lambda: defaultdict(lambda: [0, 0]))
keyword_counter = Counter()
record_count = 0
invalid_records = 0
business_ids = set()  # Track unique business_ids for rating and sentiment

response = s3_client.get_object(Bucket=bucket, Key=key)

for line in response['Body'].iter_lines():
    if not line:
        continue
    try:
        record = json.loads(line.decode('utf-8'))
        business_id = record.get('business_id')
        stars = float(record.get('stars', 0))
        text = record.get('text', "")
        date_str = record.get('date', None)

        if business_id:
            business_ids.add(business_id)
            # 1. Average rating
            ratings_sum[business_id] += stars
            ratings_count[business_id] += 1

            # 2. Sentiment
            sentiment_score = sid.polarity_scores(text)['compound']
            sentiment_sum[business_id] += sentiment_score
            sentiment_count[business_id] += 1

        # 3. Keywords
        keywords = extract_keywords(text)
        keyword_counter.update(keywords)

        # 4. Temporal trends
        if date_str:
            try:
                year = pd.to_datetime(date_str).year
                temporal_ratings[business_id][year][0] += stars
                temporal_ratings[business_id][year][1] += 1
            except ValueError:
                pass  

        record_count += 1
    except json.JSONDecodeError as e:
        invalid_records += 1
        print(f"Skipping invalid JSON line: {e}")

print(f"Processed {record_count} records, skipped {invalid_records} invalid records.")

# ---------- Save Average Ratings ----------

start_avg = time.time()
avg_ratings_data = []
for bid in ratings_sum:
    avg_rating = ratings_sum[bid] / ratings_count[bid] if ratings_count[bid] > 0 else 0
    avg_ratings_data.append({'business_id': bid, 'avg_rating': avg_rating})

avg_ratings_df = pd.DataFrame(avg_ratings_data)
csv_avg = avg_ratings_df.to_csv(index=False)
s3_client.put_object(Bucket=bucket,
                     Key='outputs/avg_ratings_sequential.csv',
                     Body=csv_avg)
metrics_list.append(log_metrics(start_avg, len(business_ids), 'Average Rating'))

# ---------- Save Sentiment Analysis ----------

start_sentiment = time.time()
avg_sentiment_data = []
for bid in sentiment_sum:
    avg_sent = sentiment_sum[bid] / sentiment_count[bid] if sentiment_count[bid] > 0 else 0
    avg_sentiment_data.append({'business_id': bid, 'avg_sentiment': avg_sent})

avg_sentiment_df = pd.DataFrame(avg_sentiment_data)
csv_sentiment = avg_sentiment_df.to_csv(index=False)
s3_client.put_object(Bucket=bucket,
                     Key='outputs/sentiment_sequential.csv',
                     Body=csv_sentiment)
metrics_list.append(log_metrics(start_sentiment, len(business_ids), 'Sentiment Analysis'))

# ---------- Save Most Common Keywords ----------

start_keywords = time.time()
top_keywords = keyword_counter.most_common(10)
keyword_df = pd.DataFrame(top_keywords, columns=['keyword', 'count'])

csv_keywords = keyword_df.to_csv(index=False)
s3_client.put_object(Bucket=bucket,
                     Key='outputs/keywords_sequential.csv',
                     Body=csv_keywords)
metrics_list.append(log_metrics(start_keywords, record_count, 'Keyword Analysis'))

# ---------- Save Temporal Trends ----------

start_temporal = time.time()
temporal_rows = []
for bid in temporal_ratings:
    for year in temporal_ratings[bid]:
        total, count = temporal_ratings[bid][year]
        avg = total / count if count > 0 else 0
        temporal_rows.append({
            'business_id': bid,
            'year': year,
            'avg_rating': avg
        })

temporal_df = pd.DataFrame(temporal_rows)
csv_temporal = temporal_df.to_csv(index=False)
s3_client.put_object(Bucket=bucket,
                     Key='outputs/temporal_trends_sequential.csv',
                     Body=csv_temporal)
metrics_list.append(log_metrics(start_temporal, record_count, 'Temporal Trends'))

# ---------- Total Metrics ----------

total_metrics = log_metrics(start_total, record_count, 'MapReduce Total')
metrics_list.append(total_metrics)

# Save all benchmark results to S3
with open('/tmp/benchmarks.json', 'w') as f:
    json.dump(metrics_list, f)
s3_client.put_object(Bucket=bucket, Key='outputs/benchmarks_sequential.json', Body=open('/tmp/benchmarks.json', 'rb'))

print("Sequential processing completed successfully!")