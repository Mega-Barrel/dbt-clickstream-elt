""" Entry Point """

from datetime import datetime

from src.pipeline import ClickstreamELTPipeline

if __name__ == '__main__':
    # Pipeline parameters
    NUM_RECORDS = 500000
    START_DATE = datetime(2025, 1, 1)
    PROJECT_ID = "analytics-engineering-101"
    DATASET_ID = "clickstream_data"
    TABLE_ID = "events"

    # Run pipeline
    pipeline = ClickstreamELTPipeline(
        num_records = NUM_RECORDS,
        start_date = START_DATE,
        project_id = PROJECT_ID,
        dataset_id = DATASET_ID,
        table_id = TABLE_ID
    )
    pipeline.run()
