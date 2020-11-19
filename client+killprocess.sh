echo "killing the server 1"
echo "killing the server 2"
sleep 3
 
kill $(lsof -i:7071 | grep LISTEN | awk '{print $2}')
kill $(lsof -i:7072 | grep LISTEN | awk '{print $2}')
bin/client -q 100000 &
sleep 10

echo "reviving server 1"
echo "reviving server 2"
bin/server -port 7070 -exec -dreply -durable &
sleep 1
bin/server -port 7071 -exec -dreply -durable &
