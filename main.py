import pandas as pd
import numpy as np
from datetime import datetime, timedelta
# from google.cloud import bigquery
import uuid

# Initialize BigQuery client
# client = bigquery.Client()

# Parameters
num_records = 1
start_date = datetime(2025, 4, 1)
# dataset_id = 'my_project.clickstream'
# table_id = f'{dataset_id}.raw_clickstream'

# Fake data generation
def generate_clickstream_data():
    event_types = ['page_view', 'click', 'form_submit', 'add_to_cart', 'purchase']
    pages = ['home', 'product', 'cart', 'checkout', 'confirmation']
    user_ids = [str(uuid.uuid4()) for _ in range(1000)]

    data = {
        'event_id': [str(uuid.uuid4()) for _ in range(num_records)],
        'user_id': np.random.choice(user_ids, num_records),
        'timestamp': [
            start_date + timedelta(seconds=np.random.randint(0, 7*24*3600))
            for _ in range(num_records)
        ],
        'event_type': np.random.choice(event_types, num_records, p=[0.4, 0.3, 0.1, 0.1, 0.1]),
        'page_url': np.random.choice(pages, num_records),
        'session_id': [str(uuid.uuid4()) for _ in range(num_records)],
        'referrer': np.random.choice(['google', 'direct', 'social', ''], num_records),
        'device': np.random.choice(['mobile', 'desktop', 'tablet'], num_records)
    }
    return data
    # return pd.DataFrame(data)

# Create and load data
# def load_to_bigquery(df):
#     table_ref = client.dataset('clickstream').table('raw_clickstream')
#     job_config = bigquery.LoadJobConfig(
#         write_disposition='WRITE_TRUNCATE',
#         schema=[
#             {'name': 'event_id', 'type': 'STRING'},
#             {'name': 'user_id', 'type': 'STRING'},
#             {'name': 'timestamp', 'type': 'TIMESTAMP'},
#             {'name': 'event_type', 'type': 'STRING'},
#             {'name': 'page_url', 'type': 'STRING'},
#             {'name': 'session_id', 'type': 'STRING'},
#             {'name': 'referrer', 'type': 'STRING'},
#             {'name': 'device', 'type': 'STRING'}
#         ]
#     )
#     client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
#     print(f"Loaded {len(df)} rows to {table_id}")

if __name__ == '__main__':
    df = generate_clickstream_data()
    print(df)
    # load_to_bigquery(df)
