import socket
import threading
import queue
from apple_health_service import what_sticks_health_service

job_queue = queue.Queue()

def run_what_sticks_health_service(user_id,time_stamp_str, add_qty_cat_bool, add_workouts_bool):
    try:
        what_sticks_health_service(user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool)
    except Exception as e:
        print(f"- its ok we caught the error: {e}")


def process_job():
    while True:
        job_data = job_queue.get()
        if job_data:
            user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool = job_data.split(',')
            run_what_sticks_health_service(user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool)
            job_queue.task_done()

def handle_client(client_socket):
    message = client_socket.recv(1024).decode('utf-8')
    job_queue.put(message)
    client_socket.sendall("Job added to queue".encode('utf-8'))
    client_socket.close()

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', 6789))
    server.listen(5)
    print("Worker script is running and waiting for jobs...")

    job_processor_thread = threading.Thread(target=process_job)
    job_processor_thread.daemon = True
    job_processor_thread.start()
    
    while True:
        client_sock, addr = server.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_sock,))
        client_thread.start()

if __name__ == "__main__":
    start_server()