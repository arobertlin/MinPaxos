echo "killing the server 2"
sleep 3
kill $(lsof -i:7072 | grep LISTEN | awk '{print $2}')
bin/clientretry -q 1 &
sleep 10

echo "reviving server 2"
bin/server -port 7072 -exec -dreply -durable &
