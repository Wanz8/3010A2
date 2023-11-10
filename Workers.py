import select
import socket
import threading
import json
import sys
from datetime import datetime

# Tweet data and locks (simulate database)
tweets = {}
tweet_locks = {}
TIMEOUT_VALUE = 60  # in seconds


def handle_request(coordinator_socket):
    ready_to_read, _, _ = select.select([coordinator_socket], [], [], TIMEOUT_VALUE)
    if ready_to_read:
        raw_data = coordinator_socket.recv(1024).decode()
        if raw_data:
            request = json.loads(raw_data)
            print(f"Raw data received: {raw_data}")
        else:
            print("No data received from coordinator.")
            return  # Exit the function if no data is received
    else:
        print("No data available to read from coordinator.")
        return
    try:
        # request_data = coordinator_socket.recv(1024).decode()
        #request = json.loads(request)
        print(request)
        # Iterate through locked tweets for timeout checks
        current_time = datetime.now().timestamp()
        for tweet_id, lock in list(tweet_locks.items()):
            if lock['timeout'] < current_time:
                del tweet_locks[tweet_id]

        # Respond to GET requests
        if request['type'] == 'GET':
            data = {tweet_id: tweet for tweet_id, tweet in tweets.items() if tweet_id not in tweet_locks}
            coordinator_socket.sendall(json.dumps(data).encode())

        # Handle SET requests (first phase of 2PC)
        elif request['type'] == 'SET':
            print("wssb")
            if 'key' in request and 'value' in request:
                tweet_id = request['key']
                tweet_data = request['value']
                print(tweet_id)
                print(tweet_data)
                if tweet_id not in tweet_locks:
                    print("timeout hahaahahahahah")
                    tweet_locks[tweet_id] = {'data': tweet_data, 'timeout': datetime.now().timestamp() + 30}
                    coordinator_socket.sendall(b'ACK')
                    print(tweet_locks)
                    coordinator_socket.send(json.dumps(request).encode())
            else:
                #print(request_data)
                print("Invalid SET request format")

        # Handle PUT and DELETE requests (also part of the first phase of 2PC)
        elif request['type'] in ['PUT', 'DELETE']:
            if 'data' in request and 'id' in request['data']:
                tweet_id = request['data']['id']
                if tweet_id not in tweet_locks:
                    tweet_locks[tweet_id] = {'timeout': current_time + 30}
                    coordinator_socket.sendall(b'ACK')

            else:
                print("Invalid PUT/DELETE request format")

        # Handle COMMIT requests (second phase of 2PC)
        elif request['type'] == 'COMMIT':
            print("Im in commit")
            # Create a list of items to iterate over
            items_to_commit = list(tweet_locks.items())
            print("Tweet locks items:", items_to_commit)
            for tweet_id, lock in items_to_commit:
                print("Tweet locks items: ", items_to_commit)
                if 'data' in lock:
                    print("Im in lock")
                    tweets[tweet_id] = lock['data']
                else:
                    print("Im not in lock")
                    tweets.pop(tweet_id, None)
            tweet_locks.clear()
            coordinator_socket.sendall(b'Commit complete')
            print(tweets)


    except json.JSONDecodeError as e:

        print(f'Error decoding JSON from coordinator: {e}', file=sys.stderr)



    finally:

        coordinator_socket.close()


def main(port):
    worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    worker_socket.bind(('', port))
    worker_socket.listen(5)
    print(f'Worker listening on port {port}')

    try:
        while True:
            coordinator_socket, addr = worker_socket.accept()
            print(f'Connected to coordinator: {addr}')
            threading.Thread(target=handle_request, args=(coordinator_socket,)).start()
    except KeyboardInterrupt:
        print('Shutting down worker...')
    except socket.error as e:
        print(f'Socket error: {e}', file=sys.stderr)
    finally:
        worker_socket.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python worker.py <port>', file=sys.stderr)
        sys.exit(1)
    port = int(sys.argv[1])
    main(port)
