import pika
import pandas as pd
from pika.exchange_type import ExchangeType
import os
import json


host = os.getenv("RABBITMQ_HOST")

def clean_data(data):
    """
    Cleans and transforms the input DataFrame.
    """
    data = data.drop_duplicates()

    # Drop GDPR data not relevant to presenter
    data = data.drop(columns=['cc_num', 'first', 'last'])

    # Convert columns to numeric
    data['amt'] = pd.to_numeric(data['amt'], errors='coerce')
    data['lat'] = pd.to_numeric(data['lat'], errors='coerce')
    data['long'] = pd.to_numeric(data['long'], errors='coerce')

    # Convert dob to age at the moment of transaction
    data['dob'] = pd.to_datetime(data['dob'], errors='coerce')

    # Convert to datetime
    data['trans_date_trans_time'] = pd.to_datetime(
        data['trans_date_trans_time'],
        format="%Y-%m-%d %H:%M:%S",
        errors='coerce'
    )


    data['age_at_trans'] = data.apply(
        lambda row: calculate_age(row['dob'], row['trans_date_trans_time']),
        axis=1
    )


    data['job_category'] = map_job_to_category(data['job'])

    # Drop irrelevant columns
    data = data.drop(columns=['Unnamed: 0', 'zip', 'merch_lat', 'merch_long', 'job', 'unix_time', 'city_pop', 'street', 'dob'])

    # Extract time features
    data['hour'] = data['trans_date_trans_time'].dt.hour
    data['day_of_week'] = data['trans_date_trans_time'].dt.dayofweek
    data['month'] = data['trans_date_trans_time'].dt.month
    data['is_weekend'] = data['day_of_week'].isin([5, 6]).astype(bool)
    data['year'] = data['trans_date_trans_time'].dt.year


    #transform isFraud to boolean
    data['is_fraud'] = data['is_fraud'].astype(bool)
    #convert datetime to str before JSON serialization
    data['trans_date_trans_time'] = data['trans_date_trans_time'].astype(str)

    return data

def calculate_age(born, ref_date):
    """
    Calculates age at the time of transaction.
    """
    if pd.isnull(born) or pd.isnull(ref_date):
        return None
    age = ref_date.year - born.year - ((ref_date.month, ref_date.day) < (born.month, born.day))
    return age

def map_job_to_category(job_series):
    """
    Maps job titles to broader job categories based on keywords.
    """
    categories = {
        "IT": ['developer', 'programmer', 'software', 'data scientist', 'IT', 'systems analyst', 'network', 'database', 'web'],
        "Engineering": ['engineer', 'architect', 'mechanical', 'electrical', 'civil'],
        "Healthcare": ['nurse', 'doctor', 'therapist', 'psychologist', 'physician', 'health', 'medical'],
        "Education": ['teacher', 'lecturer', 'tutor', 'professor', 'education'],
        "Arts": ['artist', 'illustrator', 'musician', 'actor', 'dancer', 'designer', 'photographer'],
        "Finance": ['accountant', 'banker', 'finance', 'investment', 'auditor'],
        "Legal": ['lawyer', 'solicitor', 'attorney', 'barrister', 'judge'],
        "Hospitality": ['hotel', 'restaurant', 'hospitality', 'chef', 'catering'],
        "Science": ['scientist', 'research', 'physicist', 'chemist', 'biologist'],
        "Other": []
    }

    def categorize_job(job_title):
        if pd.isnull(job_title):
            return "Other"
        job_lower = job_title.lower()
        for category, keywords in categories.items():
            if any(keyword in job_lower for keyword in keywords):
                return category
        return "Other"

    return job_series.apply(categorize_job)

def callback(ch, method, properties, body):
    """
    Callback for processing incoming messages.
    """
    records = json.loads(body)
    batch = pd.DataFrame(records)
    print(f"Received a batch of size {len(batch)}")


    cleaned_batch = clean_data(batch)

    cleaned_json = json.dumps(cleaned_batch.to_dict(orient="records"))
    ch.basic_publish(
        exchange="fraud_exchange",
        routing_key="clean_data",
        body=cleaned_json,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2
        )
    )
    print("Processed and forwarded a batch to Uploader.")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_processing():

    with pika.BlockingConnection(pika.ConnectionParameters(host)) as connection, connection.channel() as channel:
        """
        Starts the message processing loop.
        """
        print("Start Processor ...")
        channel.queue_declare(queue="raw_data_process", durable=True)
        channel.exchange_declare(
            exchange="fraud_exchange",
            exchange_type=ExchangeType.direct
        )
        channel.queue_bind(
            queue="raw_data_process",
            exchange="fraud_exchange",
            routing_key="raw_data"
        )
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue="raw_data_process", on_message_callback=callback)
        print("Waiting for raw data batches.")

        channel.start_consuming()
    
    print("Processor stopped.")


if __name__ == "__main__":
    start_processing()
