from typing import List, Dict
import pandas as pd
from prefect import task, flow

from omegaconf import DictConfig
import glob

from mlops_pbi.src.helper import load_config


# ----------------------------------------------------------------------------------------------#
#                       LOGS ,AD GROUP, USERS and WORKSPACE DATA LOADING                        #
# ----------------------------------------------------------------------------------------------#


@task(name='Load_Johns_parameters')
def load_johns_parameters(config: DictConfig) -> pd.DataFrame:
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


@task(name='Load AD Group Users')
def load_adgroups_users(config: DictConfig) -> pd.DataFrame:
    """
    Load adgroups_users into pandas dataframe
    :param config: configuration file - adgroups_users
    :return: pandas dataframe

    """
    df_azure_ADGroups_Users = pd.read_csv(config.az_adgroups_users.path, sep=config.az_adgroups_users.separator,
                                          header=config.az_adgroups_users.header,
                                          encoding=config.az_adgroups_users.encoding,
                                          on_bad_lines=config.az_adgroups_users.on_bad_lines,
                                          low_memory=config.az_adgroups_users.low_memory,
                                          names=config.az_adgroups_users.names).drop(config.az_adgroups_users.names[-1],
                                                                                     axis=1)
    return df_azure_ADGroups_Users


@task(name='Load AD Groups')
def load_adgroups(config: DictConfig) -> pd.DataFrame:
    """
    Load adgroups into pandas dataframe
    :param config: configuration file - adgroups
    :return: pandas dataframe

    """
    df_azure_ADGroups = pd.read_csv(config.az_adgroups.path, sep=config.az_adgroups.separator,
                                    header=config.az_adgroups.header,
                                    encoding=config.az_adgroups.encoding,
                                    on_bad_lines=config.az_adgroups.on_bad_lines,
                                    low_memory=config.az_adgroups.low_memory,
                                    names=config.az_adgroups.names).drop(config.az_adgroups.names[-1],
                                                                         axis=1)
    return df_azure_ADGroups


@task(name='Load AD Users')
def load_adusers(config: DictConfig) -> pd.DataFrame:
    """
    Load adusers into pandas dataframe
    :param config: configuration file - adusers
    :return: pandas dataframe

    """
    df_azure_ADUsers = pd.read_csv(config.az_adusers.path, sep=config.az_adusers.separator,
                                   header=config.az_adusers.header,
                                   encoding=config.az_adusers.encoding,
                                   on_bad_lines=config.az_adusers.on_bad_lines,
                                   low_memory=config.az_adusers.low_memory,
                                   names=config.az_adusers.names).drop(config.az_adusers.names[-1],
                                                                       axis=1)
    return df_azure_ADUsers


@task(name='Load PBI User Activity Logs',
      description='Concatenates all PBI User Activity Logs in defined directory into one dataframe')
def load_activity_logs(config: DictConfig) -> pd.DataFrame:
    """
    Load activity logs into pandas dataframe
    :param config: configuration file - activity_logs
    :return: pandas dataframe

    """
    df_activity_logs = pd.concat([pd.read_csv(config.activity_logs.name,
                                              encoding=config.activity_logs.encoding,
                                              header=config.activity_logs.header,
                                              low_memory=config.activity_logs.low_memory)
                                  for config.activity_logs.name in glob.glob(config.activity_logs.path)])

    return df_activity_logs


@task(name='Write files to csv')
def write_to_csv(**kwargs):
    config = load_config()
    for key, value in kwargs.items():
        value.to_csv(f'{config.intermediate.path}/{key}.{config.intermediate.format}', index=False)


# ----------------------------------------------------------------------------------------------#
#                       AD GROUP, USERS and WORKSPACE TRANSFORMATION                            #
# ----------------------------------------------------------------------------------------------#

@task(name='Merge ADGroups and ADGroupsUsers as adgrp_adgrpusers')
def merge_df_azure_ADGroups_Users_to_df_azure_ADGroups(df1, df2) -> pd.DataFrame:
    adgrp_adgrpusers = pd.merge(df1, df2, on='GroupId', how='left')  # .drop('Blank', axis=1)

    return adgrp_adgrpusers


@task(name='Merge adgrp_adgrpusers and johnsList as df_full_masterTable')
def merged_adgrp_adgrpusers_to_johnsList(df3, df4) -> pd.DataFrame:
    # johns_list2 = ['WorkspaceName', 'DisplayName', 'GroupId']
    df_full_masterTable = pd.merge(df3, df4, on=['DisplayName', 'GroupId'], how='outer')
    return df_full_masterTable


@task(name='Write df_full_masterTable file to csv')
def final_write_to_csv(**kwargs):
    config = load_config()
    for key, value in kwargs.items():
        value.to_csv(f'{config.final.path}/{key}.{config.final.format}', index=False)


@flow(name='Run pipeline')
def get_raw_data_sets():
    """
    Run pipeline
    :return: None

    """
    config = load_config()
    df_johns_parameters = load_johns_parameters(config)
    df_azure_ADGroups_Users = load_adgroups_users(config)
    df_azure_ADGroups = load_adgroups(config)
    df_azure_ADUsers = load_adusers(config)
    df_activity_logs = load_activity_logs(config)
    write_to_csv(johns_parameters=df_johns_parameters, adgroups_users=df_azure_ADGroups_Users,
                 adgroups=df_azure_ADGroups, adusers=df_azure_ADUsers, activity_logs=df_activity_logs)
    merged_users_userGroups = merge_df_azure_ADGroups_Users_to_df_azure_ADGroups(df_azure_ADGroups_Users,
                                                                                 df_azure_ADGroups)
    df_full_masterTable = merged_adgrp_adgrpusers_to_johnsList(merged_users_userGroups, df_johns_parameters)
    final_write_to_csv(df_full_masterTable=df_full_masterTable)



if __name__ == '__main__':
    get_raw_data_sets()
