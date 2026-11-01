#!/bin/sh
for p in $(pgrep -f "minihdfs/(namenode|datanode)\.py"); do kill -9 $p; done
exit 0
