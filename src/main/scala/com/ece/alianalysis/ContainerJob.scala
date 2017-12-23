/**
  * Created by Di on 12/5/17.
  */
package main.scala.com.ece.alianalysis

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class ContainerJob(spark: SparkSession) {
  
  def run(usagePath: String, eventPath:String, outputPath: String) = {
    val usageRecords: Dataset[ContainerUsage] = readUsageFile(usagePath);

    //usageRecords.write.parquet(outputPath + "/containerUsageRecords/")

    val eventRecords: Dataset[ContainerEvent] = readEventFile(eventPath);
    eventRecords.write.parquet(outputPath + "/containerEventRecords/")
  }
  
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
  
  def readEventFile(path: String): Dataset[ContainerEvent] = {
    val records =
      spark
        .read
        .csv(path)

    import spark.implicits._
    val utils = new Utils(spark)
    records.map{
          
      r=>{
        val cpuSeq = r.getString(7).split("|").map{ input=> utils.toInt(input)}.toSeq
        
        ContainerEvent(utils.toInt(r.getString(0)),
          r.getString(1),
          utils.toLong(r.getString(2)),
          utils.toLong(r.getString(3)),
          utils.toInt(r.getString(4)),
          utils.toFloat(r.getString(5)),
          utils.toFloat(r.getString(6)), 
          cpuSeq)
      }
    }
    
  }
}

object ContainerJob {
  def main(args: Array[String]) {
    val containerUsagePath = args(0)
    val containerEventPath = args(1)
    val outputPath = args(2)

    val spark: SparkSession = SparkSession.builder().appName("ContainerUsage").getOrCreate()
    val job = new ContainerJob(spark)
    job.run(containerUsagePath, containerEventPath, outputPath)
  }
}

