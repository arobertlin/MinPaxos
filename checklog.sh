bin/clientretry -q 1 &
sleep 3
bin/clientretry -q 1 &
sleep 3

echo "killing the server 1"
kill $(lsof -i:7071 | grep LISTEN | awk '{print $2}')
sleep 10

bin/clientretry -q 1 &
sleep 3
bin/clientretry -q 1 &
sleep 3

echo "reviving server 1"
bin/server -port 7071 -min -exec -dreply -durable &

sleep 10

bin/clientretry -q 1 &
sleep 3
bin/clientretry -q 1 &

rm stable-store*
