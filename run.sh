#!/bin/bash

lein uberjar &&
for i in {1..100}
do
  java -jar target/jepsen.nats-*-standalone.jar test --nemesis kill,partition --rate 100 --time-limit 300 --nemesis-interval 60
done
