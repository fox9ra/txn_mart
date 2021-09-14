package epk_lnk_host_mart

import org.rogach.scallop._

class CustomArgs (arguments: Seq[String]) extends ScallopConf(arguments){
  val ds = opt[String](required = true, name = "ds")
  val inputTnxTableName = opt[String](required = true, name = "input-tnx-table-name")
  val inputTnxTablePath = opt[String](required = true, name = "input-tnx-table-path")
  val inputEpklnkTableName = opt[String](required = true, name = "input-epklnk-table-name")
  val inputEpklnkTablePath = opt[String](required = true, name = "input-epklnk-table-path")
  val outputHdfsTablePath = opt[String](required = true, name = "output-hdfs-table-path")
  val outputHiveTableName = opt[String](required = true, name = "output-hive-table-name")
  verify()
}
