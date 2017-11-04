package main.scala.com.ece.alianalysis

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * Created by Di on 11/2/17.
  */
class Analysis(spark: SparkSession, eventRecords: Dataset[ServerEvent], usageRecords: Dataset[ServerUsage]) {
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
  
  def countUniqueServerUsageMachineId(): Long = {
    import spark.implicits._

    usageRecords
      .map(t => t.machineId)
      .distinct()
      .count()
  }
  
}

