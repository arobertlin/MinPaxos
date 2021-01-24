echo "killing the server 0"
echo "killing the server 2"
sleep 3
 
kill $(lsof -i:7070 | grep LISTEN | awk '{print $2}')
kill $(lsof -i:7072 | grep LISTEN | awk '{print $2}')
bin/clientretry -q 1 &
sleep 10

echo "reviving server 0"
echo "reviving server 2"
bin/server -port 7070 -exec -dreply -durable &
sleep 1
bin/server -port 7072 -exec -dreply -durable &
