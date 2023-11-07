import socket
import json

HOST = ''
PORT = 7999

tweets = {}  # Dictionary to store tweets
users = {}   # Dictionary to store user information

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    while True:
        conn, addr = s.accept()
        with conn:
            data = conn.recv(1024)
            try:
                request = json.loads(data.decode('utf-8'))

                if request['type'] == 'SET':
                    # Check if the key is for a tweet or user
                    if request['key'].startswith('tweet_'):
                        tweet_id = request['key'][6:]  # Extract the tweet ID from the key
                        tweets[tweet_id] = request['value']  # Store the tweet
                    elif request['key'].startswith('user_'):
                        username = request['key'][5:]  # Extract the username from the key
                        users[username] = request['value']  # Store the user data
                    conn.sendall('{"type": "SET-RESPONSE", "success": true}'.encode())
                elif request['type'] == 'GET':
                    # Respond to requests for tweets and users
                    if request['key'] == 'tweets':
                        reply = {"type": "GET-RESPONSE", "value": tweets}
                    elif request['key'] == 'users':
                        reply = {"type": "GET-RESPONSE", "value": users}
                    else:
                        reply = {"type": "GET-RESPONSE", "value": None}
                    conn.sendall(json.dumps(reply).encode())
                elif request['type'] == 'PUT':
                    if request['key'].startswith('tweet_'):
                        tweet_id = request['key'][6:]  # Extract the tweet ID from the key
                        if tweet_id in tweets:
                            tweets[tweet_id] = request['value']  # Update the tweet
                            conn.sendall('{"type": "PUT-RESPONSE", "success": true}'.encode())
                        else:
                            conn.sendall(
                                '{"type": "PUT-RESPONSE", "success": false, "message": "Tweet not found" }'.encode())
                elif request['type'] == 'DELETE':
                    if request['key'].startswith('tweet_'):
                        tweet_id = request['key'][7:]  # 注意：这里是7因为 "tweet_" 是6个字符加上下划线
                        if tweet_id in tweets:
                            del tweets[tweet_id]  # 删除推文
                            conn.sendall('{"type": "DELETE-RESPONSE", "success": true}'.encode())
                        else:
                            conn.sendall(
                                '{"type": "DELETE-RESPONSE", "success": false, "message": "Tweet not found"}'.encode())
                    elif request['key'] == 'user_':
                        username = request['key'][5:]
                        if username in users:
                            # 这里仅仅是删除了用户的信息，但实际注销可能需要更复杂的逻辑
                            del users[username]  # 删除用户会话
                            conn.sendall('{"type": "DELETE-RESPONSE", "success": true}'.encode())
                        else:
                            conn.sendall(
                                '{"type": "DELETE-RESPONSE", "success": false, "message": "User not found"}'.encode())

            except json.JSONDecodeError as jde:
                print("bad json! no cookie!")
                print(jde)
            except ValueError as ve:
                print("Key didn't exist")
                print(ve)
