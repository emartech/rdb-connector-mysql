#!/usr/bin/env bash

function test_mysql {
  mysqladmin ping -h db --silent
}

count=0
until ( test_mysql )
do
  ((count++))
  if [ ${count} -gt 100 ]
  then
    echo "Services didn't become ready in time"
    exit 1
  fi
  sleep 0.1
done

sbt it:test
