import socket
import threading
import json

HOST, PORT = '127.0.0.1', 8080
DB_SERVER_HOST, DB_SERVER_PORT = '127.0.0.1', 7999
# coordinator_host, coordinator_port = 'localhost', 8411
tweet_id = 0


def check_logged_in(headers):
    for header in headers:
        if header.startswith('Cookie:'):
            try:
                parts = header.split('Cookie: ')[1].split(';')
                cookies_dict = {part.split('=')[0]: part.split('=')[1] for part in parts if part}
                if cookies_dict.get('username'):
                    return True
            except IndexError:
                return False
    return False


def send_request_to_db_server(request):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((DB_SERVER_HOST, DB_SERVER_PORT))
        s.sendall(json.dumps(request).encode())
        response = s.recv(1024)
        return json.loads(response.decode('utf-8'))


def handle_client(client_socket):
    global tweet_id
    request = client_socket.recv(1024).decode('utf-8')
    headers = request.split('\r\n')

    cookies = {}
    for header in headers:
        if header.startswith('Cookie:'):
            cookie_parts = header.split('Cookie: ')[1].split(';')
            for part in cookie_parts:
                if '=' in part:
                    key, value = part.split('=', 1)
                    cookies[key.strip()] = value.strip()
            break
    method, path, _ = headers[0].split(' ')
    status_line = "HTTP/1.1 401 Unauthorized"

    if method == "GET" and path == "/":
        with open("index.html", "r") as f:
            content = f.read()
            if cookies.get('username'):
                status_line = "HTTP/1.1 200 OK"
            response = f"{status_line}\nContent-Type: text/html\n\n{content}"
            client_socket.sendall(response.encode())
    elif method in ["GET", "POST"] and path.startswith("/api/tweet"):
        if not check_logged_in(headers):
            response = "HTTP/1.1 401 Unauthorized\nContent-Type: text/html\n\n<h1>401 - Unauthorized</h1>"
            client_socket.sendall(response.encode())
        else:
            # If GET request on /api/tweet
            if method == "GET":
                response_data = send_request_to_db_server({"type": "GET", "key": "tweets"})
                if response_data is not None:
                    all_tweets = response_data.get("value", {})
                else:
                    all_tweets = {}  # If no response from DB server, return empty dictionary
                response = f"HTTP/1.1 200 OK\nContent-Type: application/json\n\n{json.dumps(all_tweets)}"
                client_socket.sendall(response.encode())
            # If POST request on /api/tweet
            elif method == "POST":
                body = headers[-1]
                tweet = json.loads(body)
                tweet['id'] = tweet_id
                send_request_to_db_server({"type": "SET", "key": f"tweet_{tweet_id}", "value": tweet})
                tweet_id += 1
                response = f"HTTP/1.1 201 Created\nContent-Type: application/json\n\n{json.dumps({'status': 'Tweet created!'})}"
                client_socket.sendall(response.encode())
    elif method == "POST" and path.startswith("/api/tweet"):
        body = headers[-1]
        tweet = json.loads(body)
        tweet['id'] = tweet_id
        send_request_to_db_server({"type": "SET", "key": f"tweet_{tweet_id}", "value": tweet})
        tweet_id += 1
        # print(headers)
        response = f"HTTP/1.1 201 Created\nContent-Type: application/json\n\n{json.dumps({'status': 'Tweet created!'})}"
        client_socket.sendall(response.encode())
    elif method == "POST" and path == "/api/login":
        body = headers[-1]
        user_data = json.loads(body)
        username = user_data.get('username')
        if username:
            send_request_to_db_server({"type": "SET", "key": f"user_{username}", "value": user_data})
            print(cookies)
        response = "HTTP/1.1 200 OK\nContent-Type: application/json\n\n{}".format(json.dumps({"status": "Logged in!"}))
        client_socket.sendall(response.encode())
        # sending JS file
    elif method == "GET" and path == "/script.js":
        with open("script.js", "r") as f:
            response = f"HTTP/1.1 200 OK\nContent-Type: text/javascript\n\n{f.read()}"
            client_socket.sendall(response.encode())
    elif method == "GET" and (path == "/main.html" or path.startswith("/main.html?")):
        with open("main.html", "r") as f:
            response = f"HTTP/1.1 200 OK\nContent-Type: text/html\n\n{f.read()}"
            client_socket.sendall(response.encode())

    elif method == "PUT" and path.startswith("/api/tweet/"):
        id = int(path.split('/')[-1])  # get the tweet ID from the path
        body = headers[-1]
        tweet_data = json.loads(body)
        '''print(body);'''
        tweet = send_request_to_db_server({"type": "GET", "key": f"tweets"}).get("value")
        tweet = tweet[str(id)]
        print(id)
        print(tweet)
        if tweet:
            tweet['content'] = tweet_data['content']
            tweet['username'] = tweet_data['username']
            send_request_to_db_server({"type": "SET", "key": f"tweet_{id}", "value": tweet})
            response = f"HTTP/1.1 200 OK\nContent-Type: application/json\n\n{json.dumps({'status': 'Tweet updated!'})}"
            client_socket.sendall(response.encode())

        else:
            response = "HTTP/1.1 404 Not Found\nContent-Type: text/html\n\n<h1>404 - Tweet Not Found</h1>"
            client_socket.sendall(response.encode())
    elif method == "DELETE" and path.startswith("/api/tweet/"):
        id = int(path.split('/')[-1])  # get the tweet ID from the path
        tweet = send_request_to_db_server({"type": "GET", "key": f"tweets"}).get("value")
        tweet = tweet[str(id)]
        if tweet:
            send_request_to_db_server({"type": "DELETE", "key": f"tweet_{id}"})
            response = f"HTTP/1.1 200 OK\nContent-Type: application/json\n\n{json.dumps({'status': 'Tweet updated!'})}"
            client_socket.sendall(response.encode())

        else:
            response = "HTTP/1.1 404 Not Found\nContent-Type: application/json\n\n{\"error\": \"Tweet not found\"}"
            client_socket.sendall(response.encode())


    elif method == "DELETE" and path == "/api/login":
        # Invalidate the cookie
        response = "HTTP/1.1 200 OK\nSet-Cookie: username=; HttpOnly; Secure; Path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT\n\n"
        client_socket.sendall(response.encode())


    else:
        response = "HTTP/1.1 404 Not Found\nContent-Type: text/html\n\n<h1>404 - Not Found</h1>"
        client_socket.sendall(response.encode())

    client_socket.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"[*] Listening on {HOST}:{PORT}")

    while True:
        client, addr = server.accept()
        print(f"[*] Accepted connection from {addr[0]}:{addr[1]}")
        client_handler = threading.Thread(target=handle_client, args=(client,))
        client_handler.start()


if __name__ == "__main__":
    main()
