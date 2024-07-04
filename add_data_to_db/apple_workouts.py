from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from ws_models import DatabaseSession, engine, AppleHealthWorkout
import os
from datetime import datetime
import sqlite3 
from common.config_and_logger import config, logger_apple
from common.utilities import wrap_up_session


# def get_existing_user_apple_workouts_data(user_id):
def make_df_existing_user_apple_workouts(user_id,pickle_apple_workouts_path_and_name):
    if os.path.exists(pickle_apple_workouts_path_and_name):
        logger_apple.info(f"- reading pickle file for workouts: {pickle_apple_workouts_path_and_name} -")
        # df_existing_user_workouts_data=pd.read_pickle(pickle_apple_workouts_path_and_name)
        df_existing_workouts=pd.read_pickle(pickle_apple_workouts_path_and_name)
        return df_existing_workouts
    else:
        logger_apple.info(f"- NO Apple Health Workouts pickle file found in: {pickle_apple_workouts_path_and_name} -")
        logger_apple.info(f"- reading Apple Workouts from WSDB into df -")
        try:
            db_session = DatabaseSession()
            query = db_session.query(AppleHealthWorkout).filter_by(user_id=user_id)
            df_existing_workouts = pd.read_sql(query.statement, engine)
            wrap_up_session(db_session)
            logger_apple.info(f"- Successfully created Apple Workouts df from WSDB -")
            return df_existing_workouts
        except SQLAlchemyError as e:
            logger_apple.info(f"An error occurred: {e}")
            return None


def add_apple_workouts_to_database(user_id,apple_workouts_filename,df_existing_user_workouts_data,pickle_apple_workouts_data_path_and_name):

    logger_apple.info(f"- accessed add_apple_workouts_to_database -")

    #create new apple_workout df
    with open(os.path.join(config.APPLE_HEALTH_DIR, apple_workouts_filename), 'r') as new_user_data_path_and_filename:
        # apple_json_data = json.load(new_user_data_path_and_filename)
        df_new_user_workout_data = pd.read_json(new_user_data_path_and_filename)

    # Convert the 'value' column in both dataframes to string
    df_new_user_workout_data['sourceVersion'] = df_new_user_workout_data['sourceVersion'].astype(str)
    df_new_user_workout_data['duration'] = df_new_user_workout_data['duration'].astype(str)
    df_new_user_workout_data['sampleType'] = df_new_user_workout_data['sampleType'].astype(str)
    df_new_user_workout_data['totalEnergyBurned'] = df_new_user_workout_data['totalEnergyBurned'].astype(str)

    df_existing_user_workouts_data['totalEnergyBurned'] = df_existing_user_workouts_data['totalEnergyBurned'].astype(str)
    df_existing_user_workouts_data['sampleType'] = df_existing_user_workouts_data['sampleType'].astype(str)
    df_existing_user_workouts_data['duration'] = df_existing_user_workouts_data['duration'].astype(str)
    df_existing_user_workouts_data['duration'] = df_existing_user_workouts_data['duration'].astype(str)

    # Perform the merge on specific columns
    df_merged = pd.merge(df_new_user_workout_data, df_existing_user_workouts_data, 
                        on=['sampleType', 'startDate', 'endDate', 'UUID'], 
                        how='outer', indicator=True)
    # Filter out the rows that are only in df_new_user_workout_data
    df_unique_new_user_data = df_merged[df_merged['_merge'] == 'left_only']
    # Drop columns ending with '_y'
    df_unique_new_user_data = df_unique_new_user_data[df_unique_new_user_data.columns.drop(list(df_unique_new_user_data.filter(regex='_y')))]
    # Filter out the rows that are duplicates (in both dataframes)
    df_duplicates = df_merged[df_merged['_merge'] == 'both']
    # Drop the merge indicator column from both dataframes
    df_unique_new_user_data = df_unique_new_user_data.drop(columns=['_merge'])
    df_duplicates = df_duplicates.drop(columns=['_merge'])
    df_unique_new_user_data['user_id'] = user_id
    # Convert 'user_id' from float to integer and then to string
    df_unique_new_user_data['user_id'] = df_unique_new_user_data['user_id'].astype(int)
    # # Drop the 'metadataAppleHealth' and 'time_stamp_utc' columns
    # df_unique_new_user_data = df_unique_new_user_data.drop(columns=['metadataAppleHealth'])
    # Fill missing values in 'time_stamp_utc' with the current UTC datetime


    # Fill missing values in 'time_stamp_utc' with the current UTC datetime
    default_date = datetime.utcnow()
    try:
        # Try to fill missing values in 'time_stamp_utc'
        df_unique_new_user_data['time_stamp_utc'] = df_unique_new_user_data['time_stamp_utc'].fillna(default_date)
    except KeyError:
        # If the column doesn't exist, create it and set all values to default_date
        df_unique_new_user_data['time_stamp_utc'] = default_date

    rename_dict = {}
    rename_dict['duration_x']='duration'
    rename_dict['sourceName_x']='sourceName'
    rename_dict['totalDistance_x']='totalDistance'
    rename_dict['device_x']='device'
    rename_dict['totalEnergyBurned_x']='totalEnergyBurned'
    rename_dict['sourceVersion_x']='sourceVersion'
    # rename_dict['quantity_x']='quantity'
    df_unique_new_user_data.rename(columns=rename_dict, inplace=True)
    count_of_records_added_to_db = 0
    try:
        ### add df to database
        count_of_records_added_to_db = df_unique_new_user_data.to_sql('apple_health_workout', con=engine, if_exists='append', index=False)
    except Exception as e:
        logger_apple.error(f"failed to add df_workouts to database")
        logger_apple.error(f"{type(e).__name__}: {e}")
    # except sqlite3.IntegrityError as e:
    #     logger_apple.info(f"An integrity error occurred: {e}")


    # Concatenate the DataFrames
    df_updated_user_apple_health = pd.concat([df_existing_user_workouts_data, df_unique_new_user_data], ignore_index=True)

    ### create pickle file  "user_0001_apple_workouts_dataframe.pkl"
    df_updated_user_apple_health.to_pickle(pickle_apple_workouts_data_path_and_name)
    # logger_apple.info("** right before error **")


    count_of_user_apple_health_records = len(df_new_user_workout_data)
    logger_apple.info(f"- count of Apple Health Workout records in db: {count_of_user_apple_health_records}")
    logger_apple.info(f"--- add_apple_workouts_to_database COMPLETE ---")
    return count_of_records_added_to_db
