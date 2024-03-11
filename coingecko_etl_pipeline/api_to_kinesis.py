import requests
from dotenv import load_dotenv
import os
import json
import boto3
import asyncio
from aiohttp import ClientSession
import time
import uuid

def extract_subset(actual_json_dictionary, selected_columns):
    extract_json_dict=[]
    extracted_json = {}
    for entry in actual_json_dictionary: 
        for column in selected_columns:
            extracted_json[column]=entry[column]
        extract_json_dict.append(extracted_json)
    return extract_json_dict

async def fetch_market_cap_data(currency, api_key, queue):
    try: # added this to cancel the task b runnin the task.cancel
        while True:
            print("Fetching data...")
            url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency={currency}&x_cg_demo_api_key={api_key}"
            async with ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        # success
                        market_cap_data = await response.json()
                        # put data in queue
                        print("Adding item to the queue...")
                        await queue.put(market_cap_data)
                        print("Item added to the queue.")
                    else:
                        # Handle the error
                        print("API request failed with status code:", response.status_code)
                    
            await asyncio.sleep(70) # 70 Seconds between calls of the API this is the time for Cache/Update Frequency for public API
    except asyncio.CancelledError:
        print("Task canceled. Exiting fetch_market_cap_data.")
             
async def send_batch_to_kinesis(stream_name, queue, kinesis_streams):
    while True:
        try:
            batch_data = await asyncio.wait_for(queue.get(), timeout=5)
        except asyncio.TimeoutError:
            if queue.empty():
                print("queue is empty, stopping task.")
                break
        else:
            try:
                for idx, data in enumerate(batch_data):
                    encoded_data = json.dumps(data).encode('utf-8')
                    #print(encoded_data)
                    kinesis_streams.send_stream(stream_name, encoded_data, None)
            except Exception as e:
                print(f"Erro while sending to kinesis : {e}" )
            
class KinesisStream():
    def __init__(self, region_name='eu-west-3'):
        self.kinesis_client = boto3.client('kinesis', region_name=region_name)
        
    def send_stream(self, stream_name, data, partition_key=None):
        if partition_key == None:
            partition_key = str(uuid.uuid4())
            
        try:
            response = self.kinesis_client.put_record(
                StreamName=stream_name,
                Data=data,
                PartitionKey=partition_key
                )
            #print("hello response is here ------------")
            #print(response)
        except self.kinesis_client.exceptions.ResourceNotFoundException:
            print(f"Kinesis stream '{stream_name}' not found")

async def main():
    load_dotenv()
    api_key = os.environ['api_key']
    queue = asyncio.Queue()
    stream_name = "api-to-kinesis-streams-coingecko"
    kinesis_streams = KinesisStream()
    
    producer_task = asyncio.create_task(fetch_market_cap_data("usd",api_key, queue))
    
    await asyncio.wait_for(producer_task, timeout=71)
    
    sending_task = asyncio.create_task(send_batch_to_kinesis(stream_name,queue,kinesis_streams))  
    
    await sending_task
    
if __name__ == "__main__":
    asyncio.run(main())