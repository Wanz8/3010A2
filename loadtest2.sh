#!/bin/bash

# Change this!
PORT=8080

for item in `seq 1 40`
do
    (echo '{"type":"SET", "key":"key", "value":"value '$item'"}' | nc -w4 localhost $PORT) &
done
wait
echo "Done setting. Most of these should fail."

# get them all (any order)

echo '{"type":"GET-DB"}' | nc -w1 localhost $PORT

wait
echo "Done"