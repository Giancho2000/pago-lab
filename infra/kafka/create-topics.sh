#!/bin/sh
set -e
B=kafka:9092
T=/opt/kafka/bin/kafka-topics.sh

# 6 particiones: permite hasta 6 consumidores en paralelo por grupo
$T --bootstrap-server $B --create --if-not-exists --topic payments.events \
   --partitions 6 --replication-factor 1 --config retention.ms=604800000      # 7 días

$T --bootstrap-server $B --create --if-not-exists --topic payments.events.retry \
   --partitions 3 --replication-factor 1

$T --bootstrap-server $B --create --if-not-exists --topic payments.events.dlt \
   --partitions 1 --replication-factor 1 --config retention.ms=2592000000     # 30 días

$T --bootstrap-server $B --list

pago-1:m1
pago-2:m1
pago-1:m2
pago-2:m2
pago-1:m3
pago-2:m3
pago-1:m4
pago-2:m4
pago-1:m5
pago-2:m5