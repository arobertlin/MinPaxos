bin/clientretry -q 1 &
sleep 3
echo "killing the server 1"
echo "killing the server 2"
kill $(lsof -i:7071 | grep LISTEN | awk '{print $2}')
kill $(lsof -i:7072 | grep LISTEN | awk '{print $2}')
sleep 10

bin/clientretry -q 1 &
