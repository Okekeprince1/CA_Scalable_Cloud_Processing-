import boto3
import json
import time
import os
import logging
from botocore.exceptions import ClientError

STREAM_NAME = "CA-Scalable-Kinesis-Stream"
REGION = "us-east-1"
BATCH_SIZE = 500
INPUT_FILE = "Scalable\\generated_datasets\\dataset_1024MB.json"
STATE_FILE = "upload_progress.json"
RETRY_LIMIT = 5
LOG_FILE = "upload_to_kinesis.log"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


kinesis_client = boto3.client("kinesis", region_name=REGION)


def load_resume_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8", errors="ignore") as f:
            state = json.load(f)
            logger.info(f"Resuming from line {state['last_line']}")
            return state["last_line"]
    return 0

def save_resume_state(line_number):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_line": line_number}, f)

def stream_file_to_kinesis():
    start_time_total = time.time()

    total_lines_sent = 0
    total_batches = 0
    batch = []
    batch_start_time = time.time()

    # Resume 
    last_line_sent = load_resume_state()
    current_line_num = 0

    try:
        with open(INPUT_FILE, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                current_line_num += 1

                if current_line_num <= last_line_sent:
                    continue 

                data = line.strip().encode("utf-8")
                if len(data) > 1024 * 1024:
                    logger.warning(f"Skipping line {current_line_num} because record exceeds 1MB limit")
                    continue
                
                parsed = json.loads(line.strip())
                partition_key = parsed.get("business_id", "default_partition")

                record = {
                    "Data": data,
                    "PartitionKey": partition_key
                }
                batch.append(record)

                # Check if batch size or total batch bytes would exceed Kinesis limit
                if len(batch) == BATCH_SIZE:
                    send_batch(batch, current_line_num)
                    total_batches += 1
                    total_lines_sent += len(batch)
                    save_resume_state(current_line_num)
                    batch = []
                    elapsed = time.time() - batch_start_time
                    throughput = BATCH_SIZE / elapsed if elapsed > 0 else 0
                    logger.info(f"Batch sent: {BATCH_SIZE} records in {elapsed:.2f}s, Throughput: {throughput:.2f} records/sec")
                    batch_start_time = time.time()

        # Send final partial batch
        if batch:
            send_batch(batch, current_line_num)
            total_batches += 1
            total_lines_sent += len(batch)
            save_resume_state(current_line_num)
            elapsed = time.time() - batch_start_time
            throughput = len(batch) / elapsed if elapsed > 0 else 0
            logger.info(f"Final batch sent: {len(batch)} records in {elapsed:.2f}s, Throughput: {throughput:.2f} records/sec")

        total_elapsed = time.time() - start_time_total
        overall_throughput = total_lines_sent / total_elapsed if total_elapsed > 0 else 0
        logger.info(f"Upload completed: {total_lines_sent} records sent in {total_elapsed:.2f}s, Overall throughput: {overall_throughput:.2f} records/sec")

        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)

    except Exception as e:
        logger.error(f"Unexpected error during streaming: {e}")
        raise


def send_batch(batch, current_line_num):
    retries = 0
    while retries < RETRY_LIMIT:
        try:
            response = kinesis_client.put_records(
                Records=batch,
                StreamName=STREAM_NAME
            )

            failed_count = response.get("FailedRecordCount", 0)
            if failed_count > 0:
                logger.warning(f"{failed_count} records failed in batch at line {current_line_num}. Retrying...")
                # Extract failed records and retry
                new_batch = []
                for i, record_response in enumerate(response["Records"]):
                    if "ErrorCode" in record_response:
                        new_batch.append(batch[i])
                batch = new_batch
                retries += 1
                time.sleep(min(2 ** retries, 30))
            else:
                return

        except ClientError as e:
            if e.response['Error']['Code'] == "ProvisionedThroughputExceededException":
                logger.warning("Throughput exceeded. Throttling...")
                retries += 1
                time.sleep(min(2 ** retries, 30))
            else:
                logger.error(f"AWS ClientError: {e}")
                retries += 1
                time.sleep(min(2 ** retries, 30))
        except Exception as e:
            logger.error(f"Unhandled error while sending batch: {e}")
            retries += 1
            time.sleep(min(2 ** retries, 30))

    if len(batch) > 0:
        logger.error(f"Failed to send {len(batch)} records after {RETRY_LIMIT} retries.")
    else:
        logger.info(f"Batch sent successfully after retries.")

if __name__ == "__main__":
    stream_file_to_kinesis()
