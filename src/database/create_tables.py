from db import connect

def create_tables():
    conn = connect()
    if conn is None:
        return

    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_data (
                transaction_id SERIAL PRIMARY KEY,
                cc_num VARCHAR(255),
                first VARCHAR(255),
                last VARCHAR(255),
                transaction_time TIMESTAMP,
                category VARCHAR(255),
                amount FLOAT,
                merchant VARCHAR(255),
                merchant_latitude FLOAT,
                merchant_longitude FLOAT,
                job VARCHAR(255),
                zip VARCHAR(255),
                gender VARCHAR(1),
                city VARCHAR(255),
                city_pop INT,
                state VARCHAR(10),
                latitude FLOAT,
                longitude FLOAT,
                unix_time BIGINT,
                is_fraud INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE processed_transactions (
            transaction_id SERIAL PRIMARY KEY,
            merchant VARCHAR(255),
            TRANSACTION_TIME TIMESTAMP,
            category VARCHAR(255),
            job_category VARCHAR(255),
            amt FLOAT,
            gender VARCHAR(1),
            city VARCHAR(255),
            state VARCHAR(10),
            is_fraud BOOLEAN,
            hour INT,
            age_at_transaction INT,
            day_of_week INT,
            month INT,
            is_weekend BOOLEAN,
            year INT,
            lat FLOAT,
            long FLOAT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Tables created.")



create_tables()
