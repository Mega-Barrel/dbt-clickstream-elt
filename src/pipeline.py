""" Python ETL for clickstream processing pipeline """

from datetime import datetime, timedelta
from typing import List

import uuid
import numpy as np

from src.config import DataConfig

class ClickstreamExtractor():
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
