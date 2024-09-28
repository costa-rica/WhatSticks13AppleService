import os
import json
from ws_models import engine, DatabaseSession, \
    AppleHealthQuantityCategory, AppleHealthWorkout, Users
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from sys import argv
import pandas as pd
import requests
from common.config_and_logger import config, logger_apple
from common.utilities import wrap_up_session, apple_health_qty_cat_json_filename, \
    apple_health_workouts_json_filename, create_pickle_apple_qty_cat_path_and_name, \
    create_pickle_apple_workouts_path_and_name
from ws_utilities import create_data_source_object_json_file, \
    create_dashboard_table_object_json_file
from add_data_to_db.apple_health_quantity_category import \
    make_df_existing_user_apple_quantity_category, add_apple_health_to_database
from add_data_to_db.apple_workouts import make_df_existing_user_apple_workouts, \
    add_apple_workouts_to_database
import queue
import threading
import time

# Define the queue and worker threads
job_queue = queue.Queue(maxsize=5)  # Conservative queue size
num_worker_threads = 2  # Conservative number of worker threads

def test_func_01(test_string):
    logger_apple.info(f"- {test_string} -")
    # add_to_apple_health_quantity_category_table(logger_apple, test_string)

def db_diagnostics():
    db_session = DatabaseSession()
    workout_db_all = sess.query(AppleHealthWorkout).all()
    qty_cat_db_all = sess.query(AppleHealthQuantityCategory).all()
    logger_apple.info(f"- AppleHealthWorkout record count: {len(workout_db_all)} -")
    logger_apple.info(f"- AppleHealthQuantityCategory record count: {len(qty_cat_db_all)} -")
    wrap_up_session(db_session)

def what_sticks_health_service(user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool):

    logger_apple.info(f"- accessed What Sticks 13 Apple Service (WSAS) -")
    logger_apple.info(f"- ******************************************* -")
    logger_apple.info(f"-- add_qty_cat_bool: {add_qty_cat_bool} -")
    logger_apple.info(f"-- add_workouts_bool: {add_workouts_bool} -")

    add_qty_cat_bool = add_qty_cat_bool == 'True'
    add_workouts_bool = add_workouts_bool == 'True'
    
    # filename json file that has new data
    apple_health_qty_cat_json_file_name = apple_health_qty_cat_json_filename(user_id, time_stamp_str)
    apple_health_workouts_json_file_name = apple_health_workouts_json_filename(user_id, time_stamp_str)

    # create pickle file name
    pickle_apple_qty_cat_path_and_name = create_pickle_apple_qty_cat_path_and_name(user_id)
    pickle_apple_workouts_path_and_name = create_pickle_apple_workouts_path_and_name(user_id)

    # create EXISTING Apple Health dfs
    df_existing_qty_cat = make_df_existing_user_apple_quantity_category(user_id, pickle_apple_qty_cat_path_and_name)
    df_existing_workouts = make_df_existing_user_apple_workouts(user_id,pickle_apple_workouts_path_and_name)

    count_of_qty_cat_records_added_to_db = 0
    count_of_workout_records_to_db = 0

    if add_qty_cat_bool and os.path.exists(os.path.join(config.APPLE_HEALTH_DIR, apple_health_qty_cat_json_file_name)):
        logger_apple.info(f"- Adding Apple Health Quantity Category Data -")
        count_of_qty_cat_records_added_to_db = add_apple_health_to_database(user_id, apple_health_qty_cat_json_file_name, 
                                            df_existing_qty_cat, pickle_apple_qty_cat_path_and_name)

    if add_workouts_bool and os.path.exists(os.path.join(config.APPLE_HEALTH_DIR, apple_health_workouts_json_file_name)):
        logger_apple.info(f"- Adding Apple Health Workouts Data -")
        count_of_workout_records_to_db = add_apple_workouts_to_database(user_id,apple_health_workouts_json_file_name,
                                            df_existing_workouts,pickle_apple_workouts_path_and_name)

    logger_apple.info(f"- count_of_qty_cat_records_added_to_db: {count_of_qty_cat_records_added_to_db} -")
    logger_apple.info(f"- count_of_workout_records_to_db: {count_of_workout_records_to_db} -")

    # create data source notify user
    if count_of_qty_cat_records_added_to_db > 0 or count_of_workout_records_to_db > 0 or time_stamp_str == "just_recalculate":
        logger_apple.info(f"- Should be making datasource json file and dashboard json files -")
        create_data_source_object_json_file(user_id, time_stamp_str)
        # create_data_source_object_json_file(user_id)
        create_dashboard_table_object_json_file(user_id)
        # call_api_notify_completion(user_id,count_of_records_added_to_db)

    logger_apple.info(f"- what_sticks_health_service Completed -")



def call_api_notify_completion(user_id,count_of_records_added_to_db):
    logger_apple.info(f"- WSAS sending WSAPI call to send email notification to user: {user_id} -")
    headers = { 'Content-Type': 'application/json'}
    payload = {}
    payload['WS_API_PASSWORD'] = config.WS_API_PASSWORD
    payload['user_id'] = user_id
    payload['count_of_records_added_to_db'] = f"{count_of_records_added_to_db:,}"
    r_email = requests.request('POST',config.API_URL + '/apple_health_subprocess_complete', headers=headers, 
                                    data=str(json.dumps(payload)))
    return r_email.status_code


######################
# Main WSAS function #
# argv[1] = user_id
# argv[2] = time stamp string for file name
# argv[3] = add_qty_cat_bool
# argv[4] = add_workouts_bool
######################
# Example of adding a job to the queue
# add_job_to_queue('user_id_example', 'time_stamp_str_example', 'True', 'True')

# Main WSAS function
if __name__ == "__main__":
    if os.environ.get('WS_CONFIG_TYPE') != 'workstation':
        # Instead of directly calling what_sticks_health_service,
        # we add the job to the queue
        add_job_to_queue(argv[1], argv[2], argv[3], argv[4])


