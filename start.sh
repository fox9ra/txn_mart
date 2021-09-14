#!/bin/bash

spark-submit \
--master yarn \
--deploy-mode cluster \
--name epk_lnk_host_mart \
--num-executors 10 \
--executor-cores 5 \
--driver-memory 10G \
--driver-cores 5 \
--executor-memory 30G \
--conf spark.yarn.executor.memoryOverhead=600 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer.max=256 \
--conf spark.shuffle.service.enabled=true \
--conf spark.shuffle.manager=sort \
--conf spark.shuffle.blockTransferService=nio \
--conf spark.sql.broadcastTimeout=2400 \
--conf spark.network.timeout=2400 \
--conf spark.dynamicAllocation.executorIdleTimeout=2400 \
--class epk_lnk_host_mart \
${TARGET_JAR} \
--ds "2021-09" \
--output-hdfs-table-path "hdfs://data/custom/rb/txn_aggr/pa/txn_aggr" \
--output-hive-table-name "epk_lnk_host_id" \
--input-tnx-table-name "txn" \
--input-tnx-table-path "hdfs://data/custom/rb/card/pa/txn" \
--input-epklnk-table-name "epk_lnk_host_id" \
--input-epklnk-table-path "hdfs://data/custom/rb/epk/pa/epk_lnk_host_id"