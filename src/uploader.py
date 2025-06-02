
import pika
import json
import os
import psycopg2
import datetime


RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT')

POSTGRES_DB=os.getenv('POSTGRES_DB')
POSTGRES_USER=os.getenv('POSTGRES_USER')
POSTGRES_PWD=os.getenv('POSTGRES_PASS')
POSTGRES_HOST=os.getenv('POSTGRES_HOST')
POSTGRES_PORT=os.getenv('POSTGRES_PORT')


def connect_to_rabbitmq():
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
    connection = pika.BlockingConnection(parameters)
    return connection


#connect to postgres
def connect_to_postgres():
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PWD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT       
    )

    conn.autocommit = False 
    return conn



def insert_raw_data(conn, data):
    """
    Inserts raw transaction data into the raw_data table.
    """
    values = []
    for record in data:
        try:
            transaction_time = record.get('trans_date_trans_time')
            transaction_time = datetime.datetime.fromisoformat(transaction_time) if transaction_time else None

            values.append((
                record.get('cc_num'),
                record.get('first'),
                record.get('last'),
                transaction_time,
                record.get('category'),
                record.get('amt'),
                record.get('merchant'),
                record.get('merch_lat'),
                record.get('merch_long'),
                record.get('job'),
                record.get('zip'),
                record.get('gender'),
                record.get('city'),
                record.get('city_pop'),
                record.get('state'),
                record.get('lat'),
                record.get('long'),
                record.get('unix_time'),
                record.get('is_fraud'),
                datetime.datetime.now()
            ))
        except Exception as e:
            print(f"Error processing record {record}: {e}")
            continue

    if not values:
        print("No valid records to insert into raw_data.")
        return

    sql = '''
        INSERT INTO raw_data (
            cc_num, first, last, transaction_time, 
            category, amount, merchant, merchant_latitude, 
            merchant_longitude, job, zip, gender, city, city_pop,
            state, latitude, longitude, unix_time, is_fraud, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting raw data: {e}")
        print("Values attempted for insertion:")
        for v in values:
            print(v)
        raise


def insert_processed_data(conn, data):
    """
    Inserts processed transaction data into the processed_transactions table.
    """
    values = []
    for record in data:
        try:
            transaction_time = record.get('trans_date_trans_time')
            transaction_time = datetime.datetime.fromisoformat(transaction_time) if transaction_time else None

            values.append((
                record.get('merchant'),
                transaction_time,
                record.get('category'),
                record.get('job_category'),
                record.get('amt'),
                record.get('gender'),
                record.get('city'),
                record.get('state'),
                bool(record.get('is_fraud')),
                record.get('hour'),
                record.get('age_at_trans'),
                record.get('day_of_week'),
                record.get('month'),
                bool(record.get('is_weekend')),
                record.get('year'),
                record.get('lat'),
                record.get('long')
            ))
        except Exception as e:
            print(f"Error processing record {record}: {e}")
            continue

    if not values:
        print("No valid records to insert into processed_transactions.")
        return

    sql = """
        INSERT INTO processed_transactions (
            merchant, transaction_time, category, job_category, amt,
            gender, city, state, is_fraud, hour, age_at_transaction,
            day_of_week, month, is_weekend, year, lat, long
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error inserting processed data: {e}")
        print("Values attempted for insertion:")
        for v in values:
            print(v)
        raise


def process_raw_data(channel, method, properties, body, db_conn):
    """
    Callback for processing raw data messages from RabbitMQ.
    """
    try:
        data = json.loads(body)
        print(f"Received {len(data)} records of raw data")

        insert_raw_data(db_conn, data)
        print(f"Inserted {len(data)} records into raw_data table")

        # Acknowledge the message
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing raw data message: {e}")
        # Nack and requeue on error
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def process_processed_data(channel, method, properties, body, db_conn):
    """
    Callback for processing processed data messages from RabbitMQ.
    """
    try:
        data = json.loads(body)
        print(f"Received {len(data)} records of processed data")

        insert_processed_data(db_conn, data)
        print(f"Inserted {len(data)} records into processed_transactions table")

        # Acknowledge the message
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing processed data message: {e}")
        # Nack and requeue on error
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)



def start_uploader():
    with pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST)) as connection, connection.channel() as channel:
        
        print("Start Uploader ...")
        
        channel.exchange_declare(exchange='fraud_exchange', exchange_type='direct')
        
        db_conn = connect_to_postgres()

        # Upload raw data
        channel.queue_declare(queue='raw_data_upload', durable=True)
        channel.queue_bind(
            queue='raw_data_upload', 
            exchange='fraud_exchange', 
            routing_key='raw_data'
        )
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue='raw_data_upload', 
            on_message_callback=lambda ch, method, properties, 
            body: process_raw_data(ch, method, properties, body, db_conn)
        )

        # Upload processed data
        channel.queue_declare(queue='processed_data_upload', durable=True)
        channel.queue_bind(
            queue='processed_data_upload', 
            exchange='fraud_exchange', 
            routing_key='clean_data'
        )
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue='processed_data_upload', 
            on_message_callback=lambda ch, method, properties, 
            body: process_processed_data(ch, method, properties, body, db_conn)
        )

        try:
            channel.start_consuming()
        finally:
            db_conn.close()
    
    print("Uploader stopped.")


if __name__ == "__main__":
    start_uploader()
        

