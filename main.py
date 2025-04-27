""" Entry Point """

from datetime import datetime

from src.pipeline import ClickstreamExtractor
from src.config import DataConfig

if __name__ == '__main__':
    # Pipeline parameter
    NUM_RECORDS = 10
    START_DATE = datetime(2025, 4, 1)
    CONFIG = DataConfig()

    # Run pipeline
    pipeline = ClickstreamExtractor(
        num_records=NUM_RECORDS,
        start_date=START_DATE,
        config=CONFIG,
    )
    data = pipeline.execute()
    print(data[0])
