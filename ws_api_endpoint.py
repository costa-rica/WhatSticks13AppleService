import subprocess

# def call_send_job( test_string):
def call_send_job(user_id,time_stamp_str, add_qty_cat_bool, add_workouts_bool):
    print("- def call_send_job( user_id,time_stamp_str, add_qty_cat_bool, add_workouts_bool)")
    # Call send_job.py with the given arguments
    subprocess.run(['python3', 'send_job.py', user_id,time_stamp_str, add_qty_cat_bool, add_workouts_bool], check=True)

# if __name__ == "__main__":
#     # Example usage
#     call_send_job(20, "Test message 20 seconds from ws_api_endpoint")
