from hydra import compose, initialize
from prefect import flow, task


# @task(name='Read_config')
def load_config():
    with initialize(version_base=None, config_path="../config"):
        config = compose(config_name="main")
    return config
