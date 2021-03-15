bin/clientretry -q 1 &
sleep 3

echo "killing the server 2"
kill $(lsof -i:7072 | grep LISTEN | awk '{print $2}')
sleep 10

echo "reviving server 2"
bin/server -port 7072 -min -exec -dreply -beacon -durable &

sleep 20

echo "killing the server 0"
kill $(lsof -i:7070 | grep LISTEN | awk '{print $2}')
sleep 10

echo "reviving server 0"
bin/server -port 7070 -min -exec -dreply -beacon -durable &
sleep 20

bin/clientretry -q 1 &
sleep 3
bin/clientretry -q 1 &

rm stable-store*
