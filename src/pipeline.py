""" Python ETL for clickstream processing pipeline """

import os
from typing import List
from datetime import datetime, timedelta

import uuid
import numpy as np
import pandas as pd

from google.cloud import bigquery
from google.api_core import exceptions
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest   #pylint: disable=E0401

from src.config import DataConfig

class ClickstreamExtract():
    """ Extracts clickstream data """
    def __init__(self, num_records: int, start_date: datetime, config: DataConfig):
        self.num_records = num_records
        self.start_date = start_date
        self.config = config
        self.user_ids = [
            str(uuid.uuid4()) for _ in range(config.NUM_USERS)
        ]

    def execute(self) -> List[dict]:
        """ Generates clickstream data as List[dict] """
        events = []
        for _ in range(self.num_records):
            event = {
                'event_id': str(uuid.uuid4()),
                'user_id': str(np.random.choice(self.user_ids, 1)[0]),
                'timestamp': self.start_date + timedelta(
                    seconds=np.random.randint(0, self.config.DATE_RANGE_SECONDS)
                ),
                'event_type': str(np.random.choice(
                    self.config.EVENT_TYPES['values'],
                    1,
                    p=self.config.EVENT_TYPES['probabilities']
                )[0]),
                'page_url': str(np.random.choice(self.config.PAGES, 1)[0]),
                'session_id': str(uuid.uuid4()),
                'referrer': str(np.random.choice(self.config.REFERENCES, 1)[0]),
                'device': str(np.random.choice(self.config.DEVICES, 1)[0])
            }
            events.append(event)
        return events
class ClickstreamLoad():
    """ Loads clickstream data to BigQuery """
    def __init__(
        self,
        data: List[dict],
        project_id: str,
        dataset_id: str,
        table_id: str
    ):
        self.data = data
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

        self.credentials_path = 'src/service_account.json'

        # Set up Google Cloud credentials
        if self.credentials_path:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"Credentials file not found at: {self.credentials_path}")
            self.credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )

        try:
            self.__client = bigquery.Client(
                credentials = self.credentials,
                project = project_id
            )
        except Exception as e:
            raise Exception(f"Failed to initialize BigQuery client: {str(e)}")

    def create_dataset_if_not_exists(self):
        """ Create dataset if it doesn't exist """
        dataset_ref = self.__client.dataset(self.dataset_id)

        try:
            self.__client.get_dataset(dataset_ref)
        except exceptions.NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = 'asia-south1'
            self.__client.create_dataset(dataset)
            print(f"Created dataset: {self.dataset_id}")

    def create_table_if_not_exists(self):
        """ Create table if it doesn't exist """
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        schema = [
            bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("page_url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("referrer", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("device", "STRING", mode="REQUIRED"),
        ]

        try:
            self.__client.get_table(table_ref)
        except exceptions.NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            self.__client.create_table(table)
            print(f"Created table: {table_ref}")

    def execute(self):
        """Load data to destination ( BigQuery DataWarehouse )"""
        # convert List[dict] to a Pandas DataFrame
        df = pd.DataFrame(self.data)

        # Create dataset and table if they don't exist
        self.create_dataset_if_not_exists()
        self.create_table_if_not_exists()

        # Load data to BigQuery
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect = False,
        )

        job = self.__client.load_table_from_dataframe(
            df,
            table_ref,
            job_config = job_config,
        )

        # Wait for job to complete
        try:
            job.result()
            print(f"Loaded {len(df)} rows to {table_ref}")
        except BadRequest as error:
            for error in job.errors:
                print(f'ERROR: {error["message"]}')

class ClickstreamELTPipeline:
    """ Orchestrates the EL (Extract and Load) process """
    def __init__(
        self,
        num_records: int,
        start_date: datetime,
        project_id: str,
        dataset_id: str,
        table_id: str
    ):
        self.num_records = num_records
        self.star_date = start_date
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.config = DataConfig()

    def run(self):
        """ Execute the Extract and Load pipeline """
        extractor = ClickstreamExtract(self.num_records, self.star_date, self.config)
        raw_data = extractor.execute()

        loader = ClickstreamLoad(
            raw_data,
            self.project_id,
            self.dataset_id,
            self.table_id
        )
        loader.execute()
