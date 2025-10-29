import requests
import boto3
import time
from typing import List, Tuple, Optional, Dict, Any
from prefect import flow, task, get_run_logger

# Configuration
UVA_ID = "esd4uq"

# AWS Region
AWS_REGION = "us-east-1"

# Target number of messages to collect
TARGET_MESSAGE_COUNT = 21

# SQS Queue URL for submitting the final answer
SUBMISSION_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

# Task 1
@task(retries=3, retry_delay_seconds=10)
def populate_queue(uva_id: str) -> str:
    """
    Sends a POST request to the API to populate the SQS queue.
    This clears all old messages and adds 21 new ones.
    """
    logger = get_run_logger()
    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uva_id}"
    
    logger.info(f"Task 1: Populating queue for '{uva_id}' at {url}...")
    
    try:
        response = requests.post(url)
        response.raise_for_status()  # Raise an error for bad responses
        payload = response.json()
        
        if 'sqs_url' in payload:
            sqs_url = payload['sqs_url']
            logger.info(f"Task 1: Success! Queue URL: {sqs_url}")
            return sqs_url
        else:
            raise ValueError("API Error: 'sqs_url' not found in response.")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Task 1: API request failed: {e}")
        raise

# Task 2

@task(retries=3, retry_delay_seconds=10, timeout_seconds=1200)
def collect_messages(queue_url: str) -> List[Tuple[int, str]]:
    """
    Monitors, receives, parses, and deletes messages from the SQS queue
    until all 21 messages have been collected.
    """
    logger = get_run_logger()
    sqs_client = boto3.client('sqs',region_name=AWS_REGION)
    
    logger.info(f"Task 2: Starting collector for queue: {queue_url}")
    logger.info(f"Task 2: Target is {TARGET_MESSAGE_COUNT} messages.")

    collected_data = []

    # Loop until we have collected the target number of messages
    while len(collected_data) < TARGET_MESSAGE_COUNT:
        try:
            # Use long-polling to wait for messages
            logger.info(
                f"Task 2: [{len(collected_data)}/{TARGET_MESSAGE_COUNT}] "
                f"Polling for messages (waiting up to 20s)..."
            )
            
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,  # Get up to 10 at a time
                WaitTimeSeconds=20,      # Long polling
                MessageAttributeNames=['order_no', 'word'] # CRITICAL: Must request attributes
            )

            if 'Messages' in response:
                messages = response['Messages']
                logger.info(f"Task 2:   -> Received {len(messages)} message(s).")

                for msg in messages:
                    # 1. Parse Data
                    attributes = msg['MessageAttributes']
                    order_no = int(attributes['order_no']['StringValue'])
                    word = attributes['word']['StringValue']
                    
                    # 2. Store Data
                    pair = (order_no, word)
                    collected_data.append(pair)
                    
                    # 3. Delete Message
                    receipt_handle = msg['ReceiptHandle']
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    
                    if len(collected_data) == TARGET_MESSAGE_COUNT:
                        logger.info("Task 2: All messages collected!")
                        break # Exit inner loop
            
            else:
                # No messages returned, just loop again (messages are likely delayed)
                logger.info("Task 2:   -> No new messages. Will poll again.")

        except Exception as e:
            logger.error(f"Task 2: Error during collection loop: {e}", exc_info=True)
            # Wait before retrying to avoid spamming on error
            time.sleep(10)
            
    logger.info("Task 2: Collection complete.")
    return collected_data

# --- Task 3: Reassemble and Submit ------------------------------------

@task
def reassemble_phrase(data: List[Tuple[int, str]]) -> str:
    """
    Sorts the collected data by 'order_no' and reassembles the phrase.
    """
    logger = get_run_logger()
    logger.info("Task 3: Reassembling phrase...")
    
    # Sort the list of tuples based on the first item (order_no)
    data.sort(key=lambda x: x[0])
    
    # Extract just the words in the correct order
    words = [item[1] for item in data]
    
    # Join the words with a space
    final_phrase = " ".join(words)
    
    logger.info(f"Task 3: Assembled phrase: {final_phrase}")
    return final_phrase

@task(retries=3, retry_delay_seconds=10)
def submit_solution(uva_id: str, phrase: str, platform: str = "prefect") -> str:
    """
    Submits the final solution to the submission SQS queue.
    """
    logger = get_run_logger()
    sqs_client = boto3.client('sqs', region_name=AWS_REGION)
    
    logger.info(f"Task 3: Submitting solution to {SUBMISSION_URL}...")
    
    try:
        response = sqs_client.send_message(
            QueueUrl=SUBMISSION_URL,
            # MessageBody is required but can be anything
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
                    'StringValue': platform
                }
            }
        )
        
        message_id = response.get('MessageId', 'N/A')
        logger.info(f"Task 3: Submit successful! MessageID: {message_id}")
        return message_id
        
    except Exception as e:
        logger.error(f"Task 3: Failed to submit solution: {e}")
        raise

# --- Main Flow --------------------------------------------------------

@flow(name="Data Project 2 - SQS Puzzle")
def data_puzzle_flow(uva_id: str = UVA_ID):
    """
    The main pipeline flow that orchestrates all tasks:
    1. Populates the queue with new messages.
    2. Collects and parses all messages.
    3. Reassembles the final phrase.
    4. Submits the solution.
    """
    logger = get_run_logger()
    logger.info(f"--- Starting Data Puzzle Flow for {uva_id} ---")
    
    # Task 1
    sqs_url = populate_queue(uva_id)
    
    # Task 2
    collected_data = collect_messages(sqs_url)
    
    # Task 3 (Part 1)
    final_phrase = reassemble_phrase(collected_data)
    
    # Task 3 (Part 2)
    submission_id = submit_solution(
        uva_id=uva_id,
        phrase=final_phrase
    )
    
    logger.info(f"--- Flow Complete ---")
    logger.info(f"Final Phrase: {final_phrase}")
    logger.info(f"Submission ID: {submission_id}")

# --- Run the Flow -----------------------------------------------------

if __name__ == "__main__":
    data_puzzle_flow(UVA_ID)