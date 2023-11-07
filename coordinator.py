import socket
import sys
import itertools

class TwoPCCoordinator:
    def __init__(self, myport, workers):
        self.myport = myport
        self.workers = itertools.cycle(workers)  # Round-robin load balancer
        self.connections = {}

    def connect_workers(self):
        for worker in self.workers:
            host, port = worker.split(':')
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = (host, int(port))
            print(f'Connecting to {server_address}')
            sock.connect(server_address)
            self.connections[worker] = sock

    def run(self):
        # Create a TCP/IP socket
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ('', self.myport)
        server_sock.bind(server_address)
        server_sock.listen(1)

        print(f'Coordinator is listening for client connections on port {self.myport}')
        while True:
            connection, client_address = server_sock.accept()
            print(f'Connection from {client_address}')
            try:
                while True:
                    data = connection.recv(1024)
                    if data:
                        command = data.decode('utf-8')
                        response = self.process_command(command)
                        connection.sendall(response.encode('utf-8'))
                    else:
                        break
            finally:
                connection.close()

    def process_command(self, command):
        print(f"Processing command: {command}")
        parts = command.split()
        cmd_type = parts[0]
        key = parts[1]
        if cmd_type in ['SET', 'GET']:
            # For simplicity, assume SET commands come in as "SET key value"
            worker_address = next(self.workers)
            worker_sock = self.connections[worker_address]
            try:
                worker_sock.sendall(command.encode('utf-8'))
                response = worker_sock.recv(1024).decode('utf-8')
                return response
            except socket.error as e:
                return f"Error communicating with worker: {e}"
        else:
            return 'UNKNOWN_COMMAND'

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: python3 coordinator.py [myport] [WorkerHost1:WorkerPort1] [WorkerHost2:WorkerPort2] ...')
        sys.exit(1)

    myport = int(sys.argv[1])
    worker_addresses = sys.argv[2:]
    coordinator = TwoPCCoordinator(myport, worker_addresses)
    coordinator.connect_workers()
    coordinator.run()
