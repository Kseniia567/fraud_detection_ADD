import pika
import pandas as pd
from pika.exchange_type import ExchangeType
import os
import json


batch_size = 1000
file_path = "../data/fraudTrain.csv"
#file_path = "../data/test.csv"


try:
    data = pd.read_csv(file_path)
except FileNotFoundError:
    print(f"File not found: {file_path}")
    exit(1)

total_length = len(data)

if total_length == 0:
    print("No data to process.")
    exit(0)


host = os.getenv("RABBITMQ_HOST")


with pika.BlockingConnection(pika.ConnectionParameters(host)) as connection, connection.channel() as channel:
    channel.exchange_declare(
        exchange="fraud_exchange", 
        exchange_type=ExchangeType.direct
        )
    
    for i in range(0, total_length, batch_size):
        batch = data[i:i + batch_size]

        batch_json = json.dumps(batch.to_dict(orient="records"))
        
        channel.basic_publish(
            exchange="fraud_exchange",
            routing_key="raw_data",
            body=batch_json,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2  
            )
        )

        print(f"Sent batch {i // batch_size + 1} of size {len(batch)}")
    
    print("All data sent successfully.")

    
    

    



