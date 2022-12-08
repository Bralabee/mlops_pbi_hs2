# import python functions to build pipeline
from first_ingestion_point import start_pipeline_pbia, format_datatype_dates, add_month_year_cols_users, \
    api_calls_filtered_out, lower_case_cols

from first_ingestion_point import lengthOfDays_since_firstRecord, lengthOfDays_since_lastActive, \
    lengthOfDays_withActivity, single_user_df, single_user_frequency, percent_time_inactiveFor

import datetime as dt
import glob
from datetime import datetime, timedelta, timezone

today = datetime.today()
now = datetime.now(timezone.utc)

from load_all_datasets import load_activity_logs
from typing import List, Dict
import pandas as pd
from prefect import task, flow
from ingest_raw_activity_log.johns_request.src.helper import load_config

from omegaconf import DictConfig


# ----------------------------------------------------------------------------------------------#
#                       LOADING LOGS DATA AND APPLYING TRANSFORMATIONS                          #
# ----------------------------------------------------------------------------------------------#


@task(name='Load Logs')
def df_activity_logs(config: DictConfig) -> pd.DataFrame:
    """
    Load John's parameters into pandas dataframe
    :param config: configuration file - johnsList
    :return: pandas dataframe

    """
    johns_parameters = pd.read_csv(config.johnsList.path, sep=config.johnsList.separator,
                                   header=config.johnsList.header,
                                   encoding=config.johnsList.encoding, on_bad_lines=config.johnsList.on_bad_lines,
                                   low_memory=config.johnsList.low_memory, names=config.johnsList.names)
    return johns_parameters


# variables for the 4th transformation node i.e. api_calls_filtered_out
service_account = 'SA_DAL_PowerBI@hs2.org.uk'
operation = 'ExportActivityEvents'


@task(name='Transform Logs')
def node_one(df):
    use_df = df \
        .pipe(start_pipeline_pbia) \
        .pipe(format_datatype_dates) \
        .pipe(add_month_year_cols_users) \
        .pipe(api_calls_filtered_out, service_account, operation) \
        .pipe(lower_case_cols, 'UserId', 'ReportName')
    return use_df


@task(name='Enhance Logs')
def node_two(df):
    use_df_2 = df.pipe(single_user_df) \
        .pipe(lengthOfDays_withActivity) \
        .pipe(lengthOfDays_since_lastActive) \
        .pipe(lengthOfDays_since_firstRecord) \
        .pipe(single_user_frequency) \
        .pipe(percent_time_inactiveFor)
    return use_df_2


@flow(name='Data_ingestion')
def data_transform():
    """
    Data ingestion flow
    :return: None
    """
    config = load_config()
    df_activity_logs = load_activity_logs(config)
    first_transform = node_one(df_activity_logs)
    second_transform = node_two(first_transform)



if __name__ == '__main__':
    data_transform()
