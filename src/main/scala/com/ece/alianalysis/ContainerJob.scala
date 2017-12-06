/**
  * Created by Di on 12/5/17.
  */
package main.scala.com.ece.alianalysis

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class ContainerJob(spark: SparkSession) {

  //def run(eventPath: String, usagePath: String, outputPath: String) = {
  def run(usagePath: String, outputPath: String) = {
    //val eventPath = "/Users/Di/Downloads/trace_201708/container_event.csv";


    //val usagePath = "/Users/Di/Downloads/trace_201708/container_usage.csv";
    val usageRecords: Dataset[ContainerUsage] = readUsageFile(usagePath);


    //val analysis = new Analysis(spark, eventRecords, usageRecords);

    /*val numOfMachinesAtSameTime: Dataset[(Integer, Int)] = analysis.numOfMachinesAtSameTime();
    numOfMachinesAtSameTime.rdd.saveAsTextFile(outputPath + "/numOfMachinesAtSameTime/")  */

    /*val serverUsageDuration: Dataset[(Integer, Int)] = analysis.serverUsageDuration();
    serverUsageDuration.rdd.saveAsTextFile(outputPath + "/serverUsageDuration/")*/

    usageRecords.write.parquet(outputPath + "/containerUsageRecords/")

    /*val cpuUsageAlongTime: Dataset[(Integer, Integer, Float)] = analysis.cpuUsageAlongTime();
    cpuUsageAlongTime.rdd.saveAsTextFile(outputPath + "/proj/racksystem-PG0/groups/jind/cpuUsageAlongTime/")*/
  }

  /*def readEventFile(path: String): Dataset[ServerEvent] = {
    val records: DataFrame =
      spark
        .read
        .csv(path)

    //println("records: " + records.count())

    import spark.implicits._

    records.map{
      r =>{
        ContainerEvent(r.getString(0).toInt,
          r.getString(1).toInt,
          r.getString(2),
          Some(r.getString(3)),
          r.getString(4).toInt,
          r.getString(5).toFloat,
          r.getString(6).toFloat)}
    }
  }*/

  def readUsageFile(path: String): Dataset[ContainerUsage] = {
    val records =
      spark
        .read
        .csv(path)

    import spark.implicits._
    val utils = new Utils(spark)

    records.map{
      r=>{
        ContainerUsage(utils.toInt(r.getString(0)),
          utils.toInt(r.getString(1)),
          utils.toFloat(r.getString(2)),
          utils.toFloat(r.getString(3)),
          utils.toFloat(r.getString(4)),
          utils.toFloat(r.getString(5)),
          utils.toFloat(r.getString(6)),
          utils.toFloat(r.getString(7)),
          utils.toFloat(r.getString(8)),
          utils.toFloat(r.getString(9)),
          utils.toFloat(r.getString(10)),
          utils.toFloat(r.getString(11)))
      }

    }
  }
}

object ContainerJob {
  def main(args: Array[String]) {
    val containerUsagePath = args(0)
    val outputPath = args(1)

    val spark: SparkSession = SparkSession.builder().appName("ContainerUsage").getOrCreate()
    val job = new ContainerJob(spark)
    job.run(containerUsagePath, outputPath)
  }
}

