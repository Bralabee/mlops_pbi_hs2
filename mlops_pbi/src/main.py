from prefect import flow

from mlops_pbi.src.load_all_datasets import get_raw_data_sets
from mlops_pbi.src.transform_logs import data_transform


@flow
def development():
    get_raw_data_sets()
    data_transform()


if __name__ == "__main__":
    development()
