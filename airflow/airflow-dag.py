# airflow DAG goes here
import requests
import boto3
import time
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin

# Configuration
UVA_ID = "esd4uq"
AWS_REGION = "us-east-1"
TARGET_MESSAGE_COUNT = 21
SUBMISSION_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

# Create a logger
log = LoggingMixin().log

# DAG Definition
@dag(
    dag_id="data_project_2_sqs_puzzle",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ds3022", "project", "sqs"]
)
def data_puzzle_dag():
    """
    Airflow DAG to solve the SQS puzzle:
    1. Populate the queue.
    2. Collect all messages.
    3. Reassemble the phrase.
    4. Submit the solution.
    """

    # Task 1
    @task(retries=3)
    def populate_queue(uva_id: str) -> str:
        """
        Sends a POST request to the API to populate the SQS queue.
        """
        url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uva_id}"
        
        log.info(f"Task 1: Populating queue for '{uva_id}' at {url}...")
        
        try:
            response = requests.post(url)
            response.raise_for_status()
            payload = response.json()
            
            if 'sqs_url' in payload:
                sqs_url = payload['sqs_url']
                log.info(f"Task 1: Success! Queue URL: {sqs_url}")
                return sqs_url
            else:
                raise ValueError("API Error: 'sqs_url' not found in response.")
                
        except requests.exceptions.RequestException as e:
            log.error(f"Task 1: API request failed: {e}")
            raise

    # Task 2
    @task(
        retries=3,
        execution_timeout=timedelta(minutes=20) 
    )
    def collect_messages(queue_url: str) -> list:
        """
        Monitors, receives, parses, and deletes messages from the SQS queue.
        """
        sqs_client = boto3.client('sqs', region_name=AWS_REGION)
        
        log.info(f"Task 2: Starting collector for queue: {queue_url}")
        log.info(f"Task 2: Target is {TARGET_MESSAGE_COUNT} messages.")

        collected_data = []

        while len(collected_data) < TARGET_MESSAGE_COUNT:
            try:
                log.info(
                    f"Task 2: [{len(collected_data)}/{TARGET_MESSAGE_COUNT}] "
                    f"Polling for messages (waiting up to 20s)..."
                )
                
                response = sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    MessageAttributeNames=['order_no', 'word']
                )

                if 'Messages' in response:
                    messages = response['Messages']
                    log.info(f"Task 2:   -> Received {len(messages)} message(s).")

                    for msg in messages:
                        attributes = msg['MessageAttributes']
                        order_no = int(attributes['order_no']['StringValue'])
                        word = attributes['word']['StringValue']
                        pair = (order_no, word)
                        collected_data.append(pair)
                        
                        receipt_handle = msg['ReceiptHandle']
                        sqs_client.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=receipt_handle
                        )
                        
                        if len(collected_data) == TARGET_MESSAGE_COUNT:
                            log.info("Task 2: All messages collected!")
                            break
                else:
                    log.info("Task 2:   -> No new messages. Will poll again.")

            except Exception as e:
                log.error(f"Task 2: Error during collection loop: {e}", exc_info=True)
                time.sleep(10)
                
        log.info("Task 2: Collection complete.")
        return collected_data

    # Task 3
    @task
    def reassemble_phrase(data: list) -> str:
        """
        Sorts the collected data by 'order_no' and reassembles the phrase.
        """
        log.info("Task 3: Reassembling phrase...")
        
        # Sort the list of tuples based on the first item (order_no)
        data.sort(key=lambda x: x[0])
        
        # Extract just the words in the correct order
        words = [item[1] for item in data]
        
        # Join the words with a space
        final_phrase = " ".join(words)
        
        log.info(f"Task 3: Assembled phrase: {final_phrase}")
        return final_phrase

    @task(retries=3)
    def submit_solution(uva_id: str, phrase: str) -> str:
        """
        Submits the final solution to the submission SQS queue.
        """
        sqs_client = boto3.client('sqs', region_name=AWS_REGION)
        
        log.info(f"Task 3: Submitting solution to {SUBMISSION_URL}...")
        
        try:
            response = sqs_client.send_message(
                QueueUrl=SUBMISSION_URL,
                MessageBody=f"Submission from {uva_id}", 
                MessageAttributes={
                    'uvaid': {
                        'DataType': 'String',
                        'StringValue': uva_id
                    },
                    'phrase': {
                        'DataType': 'String',
                        'StringValue': phrase
                    },
                    'platform': {
                        'DataType': 'String',
                        'StringValue': "airflow"
                    }
                }
            )
            
            message_id = response.get('MessageId', 'N/A')
            log.info(f"Task 3: Submit successful! MessageID: {message_id}")
            return message_id
            
        except Exception as e:
            log.error(f"Task 3: Failed to submit solution: {e}")
            raise

    # Define task dependencies
    sqs_url = populate_queue(uva_id=UVA_ID)
    collected_data = collect_messages(sqs_url)
    final_phrase = reassemble_phrase(collected_data)
    submission_id = submit_solution(
        uva_id=UVA_ID,
        phrase=final_phrase
    )

data_puzzle_dag()