from typing import List, Any

import pandas as pd
from functools import wraps
import datetime as dt
import glob
from datetime import datetime, timedelta, timezone

today = datetime.today()
now = datetime.now(timezone.utc)

'_____________________________________________________________________________________'
'------------------- Power BI Activity Log Script  SECTION 1 -------------------------'
'-------------------------------------------------------------------------------------'


def log_step_pbia(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        tic = dt.datetime.now()
        result = func(*args, **kwargs)
        time_taken = str(dt.datetime.now() - tic)
        print(f"just ran step {func.__name__} shape={result.shape} took {time_taken}s")
        return result

    return wrapper


@log_step_pbia
def start_pipeline_pbia(df) -> pd.DataFrame:
    """
        Args:
        data (dataframe)
        Returns:
        pd.DataFrame: copy
        Notes:
        This start_pipeline function is a safety mechanism.
        It guarantees that the functions will have no side effect on the raw data.
    """
    return df.copy()


@log_step_pbia
def format_datatype_dates(df) -> pd.DataFrame:
    """
     This function gets all columns with name containing time.
     It then forces these columns to adopt the python date time datatype.
     All non conformed will be coerced to NAN values as some rows may contain random values
    :param df: dataframe
    :return: dataframe
    """

    date_cols = [col for col in df.columns if 'Time' in col]
    df[date_cols] = df[date_cols].applymap(lambda x: pd.to_datetime(x, format='%Y %M %d',
                                                                    infer_datetime_format=True))
    return df


@log_step_pbia
def add_month_year_cols_users(df):
    """
        Args:
        data (dataframe)
        Returns:
        pd.DataFrame: (dataframe)
        Notes:
        This function adds month and year colunms as integers to the dataframe.
        The values are extracted from the date colunms.
    """
    return df.assign(createdDate_month=lambda d: pd.DatetimeIndex(df['CreationTime']).month,
                     createdDate_year=lambda d: pd.DatetimeIndex(df['CreationTime']).year,
                     createdDate_day=lambda d: pd.DatetimeIndex(df['CreationTime']).day)


# variables for the next function i.e. api_calls_filtered_out
service_account = 'SA_DAL_PowerBI@hs2.org.uk'
operation = 'ExportActivityEvents'


@log_step_pbia
def api_calls_filtered_out(df, the_service_account, the_operation) -> pd.DataFrame:
    """
        Args: data (dataframe), service_account (string), operation (string)
        This function filters out all rows that are not related to the defined service account and operation.
    :param df:
    :param the_service_account:
    :param the_operation:
    :return: df
    """
    filter_out_apiCalls = df.query('`Operation` != @the_operation & `UserId` != @the_service_account')
    return filter_out_apiCalls


@log_step_pbia
def lower_case_cols(df, col1, col2) -> pd.DataFrame:
    """
    This function converts two string columns to lower case
    The function takes in a dataframe and two string columns
    The function returns a dataframe with the two string columns converted to lower case
    :param df: use_df_2
    :param col1: UserId column
    :param col2: ReportName column
    :return: df
    """
    df[col1] = df[col1].str.lower()
    df[col2] = df[col2].str.lower()
    return df


'_____________________________________________________________________________________'
'------------------- Power BI Activity Log Script  SECTION 2 -------------------------'
'-------------------------------------------------------------------------------------'


@log_step_pbia
def single_user_df(df) -> pd.DataFrame:
    """
    this transformation creates a dataframe that checks the min value and max value of "CreationTime".
    an extra column is then created from a logic that -
    # A -> The difference between min and max value to see how wide or small this gap is
    # B -> Also the max value is compared to "Today" to see how long ago access to any given item was
    This will help determine if a user still needs access if the gap between last interaction and today is above defined value.
    This finally will then be used to categorise users based on [count of activities between difference of min max days.
    Lastly, check active users against their role and AAD permissions. If matched pass, else raise flag
    Check non-active users against threshold for no activity, if above treshold raise flag to withdraw or sustain permissions
    :param df:
    :return:
    """
    singleUserView = \
        df.groupby(['UserId', 'UserKey', 'WorkSpaceName', 'WorkspaceId', 'ReportName', 'ReportId', 'Activity'])[
            'CreationTime'].agg(
            ['count', 'min', 'max']).reset_index()
    return singleUserView


@log_step_pbia
def lengthOfDays_withActivity(df) -> pd.DataFrame:
    """
    This function calculates the difference between the min and max value of "CreationTime" for each user.
    This will help determine if a user still needs access if the gap between last interaction and today is above defined value.
    This finally will then be used to categorise users based on [count of activities between difference of min max days.
    :param df: dataframe
    :return: dataframe
    """
    df['lengthOfDays_withActivity'] = (((df['max'] - df['min']).astype(
        'timedelta64[D]')) + 1)  # to avoid infinity as today minus today =0

    return df


@log_step_pbia
def lengthOfDays_since_lastActive(df) -> pd.DataFrame:
    """
    This function calculates the difference between the max value of "CreationTime" and today.
    The max value is compared to "Today" to see how long ago access to any given item was.
    This will help determine if a user still needs access if the gap between last interaction and today is above defined value.
    Args:
        df (_type_): _description_
    Returns:
        pd.DataFrame: _description_
    """

    df['lengthOfDays_since_lastActive'] = ((now - df['max']).astype('timedelta64[D]')+1)

    return df


@log_step_pbia
def lengthOfDays_since_firstRecord(df) -> pd.DataFrame:
    """
    This function calculates the difference between the min value of "CreationTime" and today.
    The min value is compared to max value to see the length of time a given user was active for.
    Args:
        df (_type_): dataframe
    Returns:
        pd.DataFrame: dataframe
    """

    df['lengthOfDays_since_firstRecord'] = ((now - df['min']).astype('timedelta64[D]')+1)

    return df


@log_step_pbia
def single_user_frequency(df) -> pd.DataFrame:
    """
    This function categorises users based on [count of activities between difference of min max days.
    It is calculated by dividing the difference between lengthofDays_since_firstRecord and lengthOfDays_withActivity by the count of activities.
    The result is then rounded to 2 decimal places.
    By doing this we can see how many days a user was active for and how many activities they performed in that time.
    Args:
        df (_type_): _description_
    Returns:
        pd.DataFrame: _description_
    """
    df['frequency'] = round(((df['lengthOfDays_since_firstRecord'] - df['lengthOfDays_withActivity']) / df['count'])**2, 2)
    return df


@log_step_pbia
def percent_time_inactiveFor(df) -> pd.DataFrame:
    """
    This function calculates the percentage of time a user was inactive for.
    It is calculate by dividing the difference between lengthOfDays_since_firstRecord and lengthOfDays_withActivity by lengthOfDays_since_firstRecord
    It then rounds the value to 3 decimal places.
    By doing this we can see the percentage of time a user was inactive for.
    Args:
        df (_type_): _description_
    Returns:
        pd.DataFrame: _description_
    """
    df['%_time_inactiveFor'] = round(((df['lengthOfDays_since_firstRecord'] - df['lengthOfDays_withActivity']) / df[
        'lengthOfDays_since_firstRecord']) * 100, 3)
    return df


'_____________________________________________________________________________________'
'------------------- Power BI Activity Log Script  SECTION 3 -------------------------'
'-------------------------------------------------------------------------------------'


def compare_df_col(df1, df2, col) -> list:
    """
     This function compares a column in 2 dataframes and returns the difference as a list
     This function takes two lists as input and returns a list of users in list a that are not in list b
     This step is required to find users that are in the first node but not in the second node
     This list of users are not present after the transformation process in the second node (i.e. use_df_2),
     because they have blank workspace and/or report value assigned in the raw dataset
     This was done to ensure that the transformation process does not fail to provide all data points present in the raw
     dataset.
    :param df1: use_df dataframe from node one in first ingestion point
    :param df2: use_df_2 dataframe from node two in first ingestion point
    :param col: UserId column in both dataframes
    :return: a list of users in df1 but not in df2
    """
    df1_col = df1[col].unique().tolist()
    df2_col = df2[col].unique().tolist()
    return list(set(df1_col) - set(df2_col))


def blank_users_df(df, the_missing_user_list) -> pd.DataFrame:
    missing_users_df = df.query('UserId in @the_missing_user_list')
    return missing_users_df


def save_missing_users_df(df, path: str) -> None:
    """
    This function saves the missing users dataframe to a csv file
    :param df: missing_users_df dataframe
    :param path: location to save the csv file on the local machine
    :return: None
    """
    return df.to_csv(path, index=False)