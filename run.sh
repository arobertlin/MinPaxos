echo "installing most recent saved versions"

go install master
go install server
go install client

echo "finished installing most recent saved versions"

bin/master &
bin/server -port 7070 -exec -dreply -durable &
bin/server -port 7071 -exec -dreply -durable &
bin/server -port 7072 -exec -dreply -durable &
