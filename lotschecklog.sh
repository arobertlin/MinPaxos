bin/clientretry -q 1000000 &
sleep 3

echo "killing the server 1"
kill $(lsof -i:7071 | grep LISTEN | awk '{print $2}')
kill $(lsof -i:7072 | grep LISTEN | awk '{print $2}')
sleep 3

echo "reviving server 1"
bin/server -port 7071 -min -exec -dreply -durable &
bin/server -port 7072 -min -exec -dreply -durable &
bin/clientretry -q 1000000 &

rm stable-store*
