bin/clientretry -q 1 &
sleep 2

echo "killing the leader, server 0"
 
kill $(lsof -i:7070 | grep LISTEN | awk '{print $2}')
sleep 10

echo "reviving server 0"
bin/server -port 7070 -exec -dreply -durable &
sleep 1

# bin/clientretry -q 1 &
