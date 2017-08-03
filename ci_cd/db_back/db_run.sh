#!/bin/bash

docker run --net host --name mysql -e MYSQL_ROOT_PASSWORD=mysql -d mysql
docker run --net host --name postgres -e POSTGRES_PASSWORD=qwerty -d postgres
