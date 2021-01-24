#!/bin/bash

echo "installing most recent saved versions"
rm -rf bin/*

go install master
go install server
go install client
go install clientretry
go install genericsmr

echo "finished installing most recent saved versions"

bin/master &
bin/server -port 7070 -exec -dreply -durable &
sleep 2
bin/server -port 7071 -exec -dreply -durable &
sleep 2
bin/server -port 7072 -exec -dreply -durable &
