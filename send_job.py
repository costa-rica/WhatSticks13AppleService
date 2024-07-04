import socket
import sys


def send_job( user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 6789))
    job_data = f"{user_id},{time_stamp_str},{add_qty_cat_bool},{add_qty_cat_bool}"
    client_socket.sendall(job_data.encode('utf-8'))
    print(client_socket.recv(1024).decode('utf-8'))
    client_socket.close()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: send_job.py <user_id> <time_stamp_str> <add_qty_cat_bool> <add_workouts_bool>")
        sys.exit(1)

    user_id = sys.argv[1]
    time_stamp_str = sys.argv[2]
    add_qty_cat_bool = sys.argv[3]
    add_workouts_bool = sys.argv[4]
    send_job(user_id,time_stamp_str, add_qty_cat_bool, add_workouts_bool)
