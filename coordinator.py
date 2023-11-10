import json
import select
import socket
import threading
import time

# Global variables
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


class TransactionRequest:
    def __init__(self, web_server_socket, request, timeout):
        self.web_server_socket = web_server_socket
        self.request = request
        self.timeout = timeout
        self.start_time = time.time()
        self.state = 'init'
        self.fail = False

    def send_to_worker(self, worker_socket):
        pass

    def receive_from_worker(self, worker_socket):
        pass

    def complete_transaction(self):
        pass

    def get_web_server_socket(self):
        return self.web_server_socket

    def set_request_fail(self):
        self.fail = True

    def is_request_fail(self):
        return self.fail

    def get_state(self):
        return self.state

    def is_timed_out(self):
        return (time.time() - self.start_time) >= self.timeout

    def __str__(self):
        return str(self.request)


class Get(TransactionRequest):
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

    # def complete_transaction(self):
    #     # Send the response back to the web server
    #     response = self.request.get('response', 'No data')
    #     self.web_server_socket.sendall(response.encode())


def select_worker():
    global WORKER_INDEX
    for _ in range(len(workers)):
        worker = workers[WORKER_INDEX % len(workers)]
        WORKER_INDEX += 1
        if worker.get_worker_socket() is not None:
            return worker.get_worker_socket()
    return None


class TwoPhaseCommit(TransactionRequest):
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
        print("nsssb")
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
                        print("From receive from worker", self.data)
                        print("我commit了")
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

        self.web_server_socket.sendall(response.encode())


def handle_request_from_web_server(web_server_socket):
    while True:
        request_data = web_server_socket.recv(1024).decode()
        if not request_data:
            break

        try:
            request = json.loads(request_data)

            # 根据请求类型创建相应的事务对象
            if request['type'] == 'GET':
                transaction = Get(web_server_socket, request, timeout=30)
            elif request['type'] in ['SET', 'PUT', 'DELETE']:
                transaction = TwoPhaseCommit(web_server_socket, request, timeout=30, total_workers=len(workers))
                print("Under SET,PUT,DELETE", request['value'])

            # 将事务添加到活动事务列表
            ALL_TRANSACTION.append(transaction)

            # 选择一个工作器并发送请求
            worker_socket = select_worker()
            transaction.send_to_worker(worker_socket)

            # 将请求添加到全局请求字典
            ALL_WORKERS_REQUEST[worker_socket] = transaction

            # 等待并处理工作器的响应
            transaction.receive_from_worker(worker_socket)

            # 完成事务，将结果发送回 webServer.py
            transaction.complete_transaction()

        except json.JSONDecodeError as e:
            print("Error decoding JSON from web server:", e)
            break


workers = []
for i in range(3):  # Adjust based on your number of workers
    worker = Worker('localhost', 8000 + i)
    if worker.connect():
        workers.append(worker)
    else:
        print(f"Warning: Worker {i} could not be connected.")


def main():
    # 假设协调器监听本地主机的某个端口
    host = 'localhost'
    port = 8411  # 示例端口号

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()

        print(f"Coordinator listening on {host}:{port}")

        while True:
            web_server_socket, addr = server_socket.accept()
            print(f"Connected to web server: {addr}")
            # 启动一个新线程来处理来自web服务器的请求
            threading.Thread(target=handle_request_from_web_server, args=(web_server_socket,)).start()


if __name__ == "__main__":
    main()
