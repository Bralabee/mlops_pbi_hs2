from prefect import flow

from ingest_raw_activity_log.johns_request.src.load_all_datasets import get_raw_data_sets
from ingest_raw_activity_log.johns_request.src.transform_logs import data_transform


@flow
def development():
    get_raw_data_sets()
    data_transform()


if __name__ == "__main__":
    development()
