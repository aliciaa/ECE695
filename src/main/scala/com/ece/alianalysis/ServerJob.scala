package main.scala.com.ece.alianalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection._
import main.scala.com.ece.alianalysis.Utils

class ServerJob(spark: SparkSession) {
  
  def run(eventPath: String, usagePath: String, outputPath: String) = {
      //val eventPath = "/Users/Di/Documents/ECE695/trace_201708/server_event.csv";
      val eventRecords: Dataset[ServerEvent] = readEventFile(eventPath);
      
    
      //val usagePath = "/Users/Di/Documents/ECE695/trace_201708/server_usage.csv";
      val usageRecords: Dataset[ServerUsage] = readUsageFile(usagePath);
      
    
      val analysis = new Analysis(spark, eventRecords, usageRecords);
      val distinctServerEventMachineIdNum = analysis.countUniqueServerEventMachineId();
    
      val serverEvent = analysis.serverEventType();
      val addEvent = serverEvent.filter(e=>e._1 == "add").first()
      val softErrorEvent = serverEvent.filter(e=>e._1 == "softerror").first()
      //val hardErrorEvent = serverEvent.filter( e=> e._1 == "harderror" ).first()
      
      val serverDetailedEvent = analysis.serverDetailedEventType();
      val detailedEvent1 = serverDetailedEvent.filter(e=>e._1 == "agent_check").first()
      val detailedEvent2 = serverDetailedEvent.filter(e=>e._1 == "machine_fail").first()
      val detailedEvent3 = serverDetailedEvent.filter(e=>e._1 == "daemon_check").first()
      val detailedEvent4 = serverDetailedEvent.filter(e=>e._1 == "disk_full").first()
      val detailedEvent5 = serverDetailedEvent.filter(e=>e._1 == "nc_diskless").first()
    
      val cpuCapacityInfo = analysis.cpuCapacity();
      val cpuCap1 = cpuCapacityInfo.filter(e=>e._1 == 0).first()
      val cpuCap2 = cpuCapacityInfo.filter(e=>e._1 == 64).first()
    
      val memoryInfo = analysis.memoryInfo();
      val diskInfo = analysis.diskInfo();

    
      val serverEventResult: RDD[ServerResults] = 
        spark
          .sparkContext
          .parallelize[ServerResults](
          Seq(ServerResults(
            distinctServerEventMachineIdNum,
            EventCount(addEvent._1, addEvent._2),
            EventCount(softErrorEvent._1, softErrorEvent._2),
            EventCount(detailedEvent1._1, detailedEvent1._2),
            EventCount(detailedEvent2._1, detailedEvent2._2),
            EventCount(detailedEvent3._1, detailedEvent3._2),
            EventCount(detailedEvent4._1, detailedEvent4._2),
            EventCount(detailedEvent5._1, detailedEvent5._2),
            EventCount(cpuCap1._1.toString, cpuCap1._2),
            EventCount(cpuCap2._1.toString, cpuCap2._2),
            memoryInfo.toString, diskInfo.toString)))
        
      serverEventResult.saveAsTextFile("./output/")
      
      val numOfMachinesAtSameTime: Dataset[(Integer, Int)] = analysis.numOfMachinesAtSameTime();
      numOfMachinesAtSameTime.rdd.saveAsTextFile(outputPath + "/numOfMachinesAtSameTime/")  
    
      val serverUsageDuration: Dataset[(Integer, Int)] = analysis.serverUsageDuration();
      serverUsageDuration.rdd.saveAsTextFile(outputPath + "/serverUsageDuration/")
    
      usageRecords.write.parquet(outputPath + "/usageRecords/")
    
      val cpuUsageAlongTime: Dataset[(Integer, Integer, Float)] = analysis.cpuUsageAlongTime();
      cpuUsageAlongTime.rdd.saveAsTextFile(outputPath + "/proj/racksystem-PG0/groups/jind/cpuUsageAlongTime/")
  }

  def readEventFile(path: String): Dataset[ServerEvent] = {
    val records: DataFrame =
      spark
        .read
        .csv(path)
    
    //println("records: " + records.count())
    
    import spark.implicits._
    
    records.map{
      r =>{
        //println(r.toString())
        ServerEvent(r.getString(0).toInt, 
          r.getString(1).toInt, 
          r.getString(2),
          Some(r.getString(3)), 
          r.getString(4).toInt,
          r.getString(5).toFloat, 
          r.getString(6).toFloat)}
    }
  }

  def readUsageFile(path: String): Dataset[ServerUsage] = {
    val records: DataFrame =
      spark
        .read
        .csv(path)
    
    import spark.implicits._
    val utils = new Utils(spark)
    
    records.map{
      r=>{
        ServerUsage(utils.toInt(r.getString(0)),
          utils.toInt(r.getString(1)),
          utils.toFloat(r.getString(2)),
          utils.toFloat(r.getString(3)),
          utils.toFloat(r.getString(4)),
          utils.toFloat(r.getString(5)),
          utils.toFloat(r.getString(6)),
          utils.toFloat(r.getString(7)))
      }
      
    }
  }
}

object ServerJob {
  def main(args: Array[String]) {
    val serverEventPath = args(0)
    val serverUsagePath = args(1)
    val outputPath = args(2)

    val spark: SparkSession = SparkSession.builder().appName("ServerEvent").getOrCreate()
    val job = new ServerJob(spark)
    val results = job.run(serverEventPath, serverUsagePath, outputPath)
  }
}
