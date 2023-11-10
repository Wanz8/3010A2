#!/bin/bash

# Change this!
PORT=8080

for item in `seq 1 30`
do
    (echo '{"type":"SET", "key":"key '${item}'", "value":"value '$item'"}' | nc -w4 localhost $PORT) &
done
wait
echo "Done setting. Should have taken less than four seconds"

# do an update

for item in `seq 1 30`
do
    (echo '{"type":"SET", "key":"key '${item}'", "value":"updated value '$item'"}' | nc -w4 localhost $PORT) &
done
wait
echo "Done updating. Should have taken less than four seconds"

# get them all (any order)

echo '{"type":"GET-DB"}' | nc -w1 localhost $PORT

wait
echo "Done"