package epk_lnk_host_mart

import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.text.SimpleDateFormat

object epk_lnk_host_mart extends App {

  val conf = new CustomArgs(args)
  val ds = conf.ds()
  val inputTnxTableName = conf.inputTnxTableName()
  val inputTnxTablePath = conf.inputTnxTablePath()
  val inputEpklnkTableName = conf.inputEpklnkTableName()
  val inputEpklnkTablePath = conf.inputEpklnkTablePath()
  val outputHdfsTablePath = conf.outputHdfsTablePath()
  val outputHiveTableName = conf.outputHiveTableName()

  val spark: SparkSession = SparkSession.builder
    .master("yarn")
    .appName("epk_lnk_host_mart ")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.authenticate.secret", "doesn't matter")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.rpc.message.maxSize", 2000)
    .config("spark.driver.memory", "2g")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.sql.execution.arrow.enabled", "true")
    .config("spark.dynamicAllocation.minExecutors", 1)
    .config("spark.dynamicAllocation.maxExecutors", 10)
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.default.parallelism", "1000")
    .config("spark.sql.shuffle.partitions", "1000")
    .enableHiveSupport()
    .getOrCreate()

  //   val ds = "2021-09"

  import spark.implicits._

  val sqlContext = spark.sqlContext

  val nominalDate = new SimpleDateFormat("yyyy-MM").parse(ds)

  //first day of current month
  val DATE_01 = DateUtils.setDays(nominalDate, 1)

  //first day of the next months
  val DATE_1M_xx = DateUtils.addMonths(DATE_01, 1)

  //last day of current months
  val DATE_xx = DateUtils.addDays(DATE_1M_xx, -1)

  //last day of current month hdp fomat
  val DATE_1M_xx_hdp = new SimpleDateFormat("yyyy-MM-dd").format(DATE_xx)


  val inputTnxTablePathFull = inputTnxTablePath + "/trx_date=" + ds + "*/*.parquet"
  val inputEpklnkTablePathFull = inputEpklnkTablePath + "/row_actual_to=9999-12-31/*.parquet"

  var tnxDf = spark.read.parquet(inputTnxTablePathFull)
    .select("evt_id",
      "evt_tim",
      "client_w4_id",
      "mcc_code",
      "local_amt"
     )
    .toDF

  val EpklnkDf = spark.read.parquet(inputEpklnkTablePathFull)
    .select("epk_id",
      "external_system",
      "external_system_client_id",
      "row_actual_from"
    )
    .filter('external_system === "WAY4")
    .toDF

  val result = tnxDf
    .join(EpklnkDf, 'client_w4_id === 'external_system_client_id, "left")
    .withColumn("report_dt", DATE_1M_xx_hdp)
    .select(
      'epk_id,
      'mcc_code,
      'local_amt,
      'report_dt)
    .groupBy('epk_id, 'mcc_code, 'report_dt)
    .agg(sum('local_amt).as("sum_txn"))
    .toDF

  val outputHdfsTablePathNew = outputHdfsTablePath + "/report_dt=" + DATE_1M_xx_hdp

  result
    .repartition(30)
    .write
    .mode("overwrite")
    .format("parquet")
    .save(outputHdfsTablePathNew)

  spark.sql(s"""alter table
    $outputHiveTableName
    add IF NOT EXISTS
    partition (report_dt='$DATE_1M_xx_hdp')
  location '$outputHdfsTablePathNew'""")

}
