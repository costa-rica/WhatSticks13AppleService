from common.config_and_logger import config, logger_apple
from common.utilities import wrap_up_session
from datetime import datetime
import os
# import pandas as pd
import json

def save_job_to_queue_json(job_dict):
    filename_and_path = os.path.join(config.DIR_APPLE_SERVICE_HELPERS,'current_jobs.json')
    
    new_job_id = f"{job_dict.get('user_id')}-{job_dict.get('time_stamp_str')}"
    # Check if the file exists, if not create an empty list
    if os.path.exists(filename_and_path):
        with open(filename_and_path, 'r') as file:
            jobs_dict = json.load(file)
            jobs_dict = {**jobs_dict, new_job_id:job_dict}# "**" unpacks the dictionary jobs_dict
    else:
        jobs_dict = {new_job_id:job_dict}
    
    # Save the updated list back to the JSON file
    with open(filename_and_path, 'w') as file:
        json.dump(jobs_dict, file, indent=4)

def remove_job_from_queue_json(job_dict):
    logger_apple.info(f"- in remove_job_from_queue_json")
    logger_apple.info(f"- 📢 in remove_job_from_queue_json the job_dict is: {job_dict}")
    remove_job_id = f"{job_dict.get('user_id')}-{job_dict.get('time_stamp_str')}"
    filename_and_path = os.path.join(config.DIR_APPLE_SERVICE_HELPERS, 'current_jobs.json')
    completed_jobs_path = os.path.join(config.DIR_APPLE_SERVICE_HELPERS, 'completed_jobs.json')
    
    # Check if current_jobs.json exists
    if os.path.exists(filename_and_path):
        with open(filename_and_path, 'r') as file:
            jobs_dict = json.load(file)
        
        # Check if the job exists in current_jobs.json
        if remove_job_id in jobs_dict:
            # Save the job to be removed
            jobs_dict.pop(remove_job_id)
            removed_job = {**job_dict, "apple_service_status":"job completed"}

            # Save the updated jobs_dict back to current_jobs.json
            with open(filename_and_path, 'w') as file:
                json.dump(jobs_dict, file, indent=4)
            
            logger_apple.info(f"Job with {remove_job_id} removed from current_jobs.json.")
            
        else:
            removed_job = {**job_dict, "apple_service_status":"job never completed"}
            logger_apple.info(f"Job with {remove_job_id} not found in current_jobs.json.")

        # Handle completed_jobs.json
        if os.path.exists(completed_jobs_path):
            # Load the existing completed_jobs.json
            with open(completed_jobs_path, 'r') as file:
                completed_jobs_dict = json.load(file)
        else:
            # Create an empty dict if the file does not exist
            completed_jobs_dict = {}

        # Add the removed job to completed_jobs.json
        completed_jobs_dict[remove_job_id] = removed_job
        
        # Save the updated completed_jobs.json
        with open(completed_jobs_path, 'w') as file:
            json.dump(completed_jobs_dict, file, indent=4)
        
        logger_apple.info(f"Job with {remove_job_id} added to completed_jobs.json.")
    else:
        logger_apple.info("current_jobs.json does not exist.")


