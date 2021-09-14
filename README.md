# txn_mart

## src_tbl `txn`  PARTITIONED BY (`trx_date` string) --Партиционирование по дате транзакции в формате yyyy-mm-dd

|column|type|desc|
|------|----|----|
|evt_id|bigint| --id события, уникальный ключ|
|evt_tim|timestamp| --timestamp события|
|client_w4id|bigint| --уникальный идентификатор клиента|
|mcc_code|integer| --mcc код транзакции|
|local_amt|decimal(28,10)| --сумма транзакции|

## dictionary tbl `epk_lnk_host_id` PARTITIONED BY ( `row_actual_to` string) --дата окончания актуальности записи, нас интересуют только актуальные id клиента, т.е. где row_actual_to = ‘9999-12-31’

|column|type|desc|
|------|----|----|
|epk_id|bigint||
|external_system|string| --Наименование системы источника, тут фльтровать нужно по 'WAY4'|
|external_system_client_id|string| --id клиента в системе источника|
|row_actual_from|string| --дата начла актуальности записи|

## result tbl `epk_lnk_host_mart` PARTITIONED BY ( `report_dt` string) --отчетная дата, конец месяца в формате yyyy-mm-dd

|column|type|desc|
|------|----|----|
|epk_id|bigint||
|sum_txn|decimal(28,10)| --Сумма транзакций|
|mcc_code|integer| --mcc код транзакций|
