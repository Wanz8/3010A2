import json
import select
import socket
import threading
import time

ALL_WORKERS_REQUEST = {}
ALL_TRANSACTION = []
WORKER_INDEX = 0


class Worker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.worker_socket = None

    def connect(self):
        try:
            self.worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.worker_socket.connect((self.host, self.port))
            self.worker_socket.setblocking(False)
            print(f"Connect to worker {self.host}:{self.port}")
            return True
        except socket.error:
            print(f"Fail to connect to worker {self.host}:{self.port}")
            self.worker_socket = None
            return False

    def get_worker_socket(self):
        return self.worker_socket


class Handle_Request:
    def __init__(self, web_server_socket, request, timeout):
        self.web_server_socket = web_server_socket
        self.request = request
        self.timeout = timeout
        self.start_time = time.time()
        self.state = 'init'
        self.fail = False

    def complete_transaction(self):
        pass

    def set_request_fail(self):
        self.fail = True

    def __str__(self):
        return str(self.request)


class Get(Handle_Request):
    def send_to_worker(self, worker_socket):
        if worker_socket is None:
            self.set_request_fail()
            self.state = 'failed'
            return
        else:
            # worker_socket is now an actual socket object
            worker_socket.sendall(str(self.request).encode())
            self.state = 'waiting'

    def receive_from_worker(self, worker_socket):
        if self.state in ['waiting', 'committing']:
            ready_to_read, _, _ = select.select([worker_socket], [], [], self.timeout)
            if ready_to_read:
                response = worker_socket.recv(1024).decode()
                if not response:
                    self.set_request_fail()
                    self.state = 'failed'
                else:
                    self.request['response'] = response
                    self.state = 'received'
        else:
            print(self.state)
            print("No data available from worker.")

    def complete_transaction(self):
        if self.state == 'received':
            response = self.request['response']
            self.web_server_socket.sendall(json.dumps(response).encode())
        else:
            response = 'Get failed'
            self.web_server_socket.sendall(json.dumps(response).encode())


def select_worker():
    global WORKER_INDEX
    for _ in range(len(workers)):
        worker = workers[WORKER_INDEX % len(workers)]
        WORKER_INDEX += 1
        if worker.get_worker_socket() is not None:
            return worker.get_worker_socket()
    return None


class twoPhaseCommit(Handle_Request):
    def __init__(self, web_server_socket, request, timeout, total_workers):
        super().__init__(web_server_socket, request, timeout)
        self.total_workers = total_workers
        self.commit_count = 0
        self.data = request.get('data')  # Store the data for later use
        print("Transaction initialization with data:", self.data)

    def send_to_worker(self, worker_socket):
        try:
            if worker_socket is None:
                self.set_request_fail()
                self.state = 'failed'
                return

            # Use json.dumps to serialize the request
            worker_socket.sendall(json.dumps(self.request).encode())
            self.state = 'waiting'
        except BrokenPipeError as e:
            print(f"Error sending data to worker: {e}")
            self.set_request_fail()
            self.state = 'failed'

    def receive_from_worker(self, worker_socket):
        #print("nsssb")
        print(self.state)
        if self.state in ['waiting', 'committing']:
            ready_to_read, _, _ = select.select([worker_socket], [], [], self.timeout)
            if ready_to_read:
                response = worker_socket.recv(1024).decode()
                # Check if any response is received
                print(response)
                if response:
                    if response == 'ACK':
                        self.commit_count += 1
                        # if self.commit_count == self.total_workers:
                        self.state = 'committed'
                        print("I already committed")
                    else:
                        self.set_request_fail()
                        self.state = 'failed'
                else:
                    print("Received empty response from worker.")
                    self.set_request_fail()
                    self.state = 'failed'
            else:
                print("No data available from worker.")
                self.set_request_fail()
                self.state = 'failed'

    def complete_transaction(self):
        if self.state == 'committed':
            # Check if 'data' key exists in the request
            print(self.request)
            self.data = self.request.get('value')
            print(self.data)
            if self.data:
                # Logic to instruct workers to save the tweet
                for worker in workers:
                    worker_socket = worker.get_worker_socket()
                    if worker_socket:
                        try:
                            worker_socket.sendall(json.dumps({'type': 'COMMIT', 'data': self.data}).encode())
                        except BrokenPipeError as e:
                            print(f"Error sending save request to worker: {e}")
                response = 'Commit successful'
            else:
                print("No data found to save-From complete_transaction")
                response = 'Commit failed - No data to save'

        else:
            response = 'Commit failed'

        self.web_server_socket.sendall(json.dumps(response).encode())


def handle_request_from_web_server(web_server_socket):
    while True:
        request_data = web_server_socket.recv(1024).decode()
        if not request_data:
            break

        try:
            request = json.loads(request_data)

            if request['type'] == 'GET':
                transaction = Get(web_server_socket, request, timeout=30)
            elif request['type'] in ['SET', 'PUT', 'DELETE']:
                transaction = twoPhaseCommit(web_server_socket, request, timeout=30, total_workers=len(workers))
                print("Under SET,PUT,DELETE", request['value'])

            ALL_TRANSACTION.append(transaction)

            worker_socket = select_worker()
            transaction.send_to_worker(worker_socket)

            ALL_WORKERS_REQUEST[worker_socket] = transaction

            transaction.receive_from_worker(worker_socket)

            # Commit the transaction if it is ready
            transaction.complete_transaction()

        except json.JSONDecodeError as e:
            print("Error decoding JSON from web server:", e)
            break


workers = []
for i in range(3):
    worker = Worker('localhost', 8000 + i)
    if worker.connect():
        workers.append(worker)
    else:
        print(f"Warning: Worker {i} could not be connected.")


def main():
    host = 'localhost'
    port = 8411

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()

        print(f"Coordinator listening on {host}:{port}")

        while True:
            web_server_socket, addr = server_socket.accept()
            print(f"Connected to web server: {addr}")
            threading.Thread(target=handle_request_from_web_server, args=(web_server_socket,)).start()


if __name__ == "__main__":
    main()
