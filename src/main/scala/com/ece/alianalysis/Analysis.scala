package main.scala.com.ece.alianalysis

import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, SparkSession}


/**
  * Created by Di on 11/2/17.
  */
class Analysis(spark: SparkSession, eventRecords: Dataset[ServerEvent], usageRecords: Dataset[ServerUsage]) {
  //This part is about event records
  def countUniqueServerEventMachineId(): Long = {
    import spark.implicits._

    eventRecords
      .map(t => t.machineId)
      .distinct()
      .count()
  }


  def serverEventType(): Dataset[(String, Int)] = {
    import spark.implicits._
    
    eventRecords
      .map(r => (r.eventType, 1))
      .groupByKey(_._1)
      .mapGroups {
        case (event: String, iter: Iterator[(String, Int)]) => (event, iter.map(_._2).sum)
      }
  }
  
  def serverDetailedEventType(): Dataset[(String, Int)] = {
    import spark.implicits._
    eventRecords
      .map(r=>r.eventDetail.getOrElse(""))
      .filter( r=> r.length() > 5)
      .map( r=> (r, 1))
      .groupByKey(_._1)
      .mapGroups{
        case(event: String, iter: Iterator[(String, Int)]) => (event, iter.map(_._2).sum)
      }
    /*eventRecords
        .map(r=>(r.eventDetail.getOrElse(""), 1))
        .filter( r => r._1.length() > 5)
        .groupByKey(_._1)
        .mapGroups{
          case(event: String, iter: Iterator[(String, Int)]) => (event, iter.map(_._2).sum)
        }*/
  }

  def cpuCapacity(): Dataset[(Integer, Int)] = {
    import spark.implicits._
    eventRecords
      .map(r => (r.numberOfCpus, 1))
      .groupByKey(_._1)
      .mapGroups{
        case(cpuNumber: Integer, iter: Iterator[(Integer, Int)]) => (cpuNumber, iter.map(_._2).sum)
      }
  }
  
  def memoryInfo(): StatsInfo = {
    import spark.implicits._
    val stats = eventRecords.map(r=> r.normalizedMemory).describe()
    
    val count = stats.filter(e=> e.getString(0) ==  "count").first().getString(1).toLong
    val mean = stats.filter(e=> e.getString(0) ==  "mean").first().getString(1).toFloat
    val stddev = stats.filter(e=> e.getString(0) ==  "stddev").first().getString(1).toFloat
    val min = stats.filter(e=> e.getString(0) ==  "min").first().getString(1).toFloat
    val max = stats.filter(e=> e.getString(0) ==  "max").first().getString(1).toFloat
    StatsInfo("Memory Info", count, mean, stddev, min, max)
  }
  
  
  def diskInfo(): StatsInfo = {
    import spark.implicits._
    val stats = eventRecords.map(r=>r.normalizedDiskSpace).describe()

    val count = stats.filter(e=> e.getString(0) ==  "count").first().getString(1).toLong
    val mean = stats.filter(e=> e.getString(0) ==  "mean").first().getString(1).toFloat
    val stddev = stats.filter(e=> e.getString(0) ==  "stddev").first().getString(1).toFloat
    val min = stats.filter(e=> e.getString(0) ==  "min").first().getString(1).toFloat
    val max = stats.filter(e=> e.getString(0) ==  "max").first().getString(1).toFloat
    StatsInfo("disk Info", count, mean, stddev, min, max)
  }
  
  
  //this part is about usage records
  def countUniqueServerUsageMachineId(): Long = {
    import spark.implicits._

    usageRecords
      .map(t => t.machineId)
      .distinct()
      .count()
  }
  
  //return value is time, # of active machines
  def numOfMachinesAtSameTime(): Dataset[(Integer, Int)] = {
    import spark.implicits._
    
    usageRecords
      .groupByKey(_.timeStamp)
      .mapGroups{
        case (time, records) => (time, records.map(r=>r.machineId).toSeq.distinct.length) 
    }
  }
  
  //return value is machineID, jobDuration
  def serverUsageDuration(): Dataset[(Integer, Int)] = {
    import spark.implicits._
    
    usageRecords
      .groupByKey(_.machineId)
      .mapGroups{
        case(machineId, records) => (machineId, records.toSeq.sortBy(_.timeStamp).map(r=>r.timeStamp))
      }
      .map{
        case(machineId: Integer, jobDurations: Seq[Integer]) => (machineId, jobDurations.last-jobDurations.head)
      }
    
    
  }
  
  //return value is machineID, timeStamp, usedPercentOfCpus
  def cpuUsageAlongTime(): Dataset[(Integer, Integer, Float)] ={
    import spark.implicits._
    
    usageRecords
      .groupByKey(_.machineId)
      .mapGroups{
        case(machineId, records) => (machineId, records.toSeq.sortBy(_.timeStamp).map(r=>(r.timeStamp, r.usedPercentOfCpus)))
      }
      .flatMap{
        case(machineId: Integer, records: Seq[(Integer, Float)]) => records.map( r=> (machineId, r._1, r._2))
      }
  }
}

