import socket
import threading
import sys


class TwoPCWorker:
    def __init__(self, port):
        self.locks = {}  # To manage locks on resources (tweets)
        self.data = {}  # In-memory data store
        self.port = port

    def run(self):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the address given on the command line
        server_address = ('', self.port)
        sock.bind(server_address)
        sock.listen(1)

        while True:
            print(f'waiting for a connection on port {self.port}')
            connection, client_address = sock.accept()
            print(f'connection from {client_address}')

            try:
                threading.Thread(target=self.handle_request, args=(connection,)).start()
            except Exception as e:
                print(f'Error: {e}')

    def handle_request(self, connection):
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
        parts = command.split()
        cmd_type = parts[0]

        if cmd_type == 'PREPARE':
            # Lock the resource (tweet) for update
            key = parts[1]
            if key in self.locks:
                return 'RESOURCE_BUSY'
            else:
                self.locks[key] = True
                return 'READY'

        elif cmd_type == 'COMMIT':
            # Commit the update to the resource
            key = parts[1]
            value = parts[2]
            if key in self.locks:
                self.data[key] = value
                del self.locks[key]  # Unlock the resource after commit
                return 'COMMIT_OK'
            else:
                return 'NOT_LOCKED'

        elif cmd_type == 'ABORT':
            # Abort the transaction and unlock the resource
            key = parts[1]
            if key in self.locks:
                del self.locks[key]
            return 'ABORT_DONE'

        elif cmd_type == 'GET':
            # Fetch the value of the resource
            key = parts[1]
            return self.data.get(key, 'NOT_FOUND')

        else:
            return 'UNKNOWN_COMMAND'


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python3 worker.py [port]')
        sys.exit(1)

    port = int(sys.argv[1])
    worker = TwoPCWorker(port)
    worker.run()
