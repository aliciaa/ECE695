package main.scala.com.ece.alianalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection._


class ServerJob(spark: SparkSession) {
  //implicit def kryoEncoder[ServerEvent](implicit ct: ClassTag[ServerEvent]) =
   // org.apache.spark.sql.Encoders.kryo[ServerEvent](ct)
  
  // reads data from text files and computes the results. This is what you test
  def run(serverEventPath: String) = {
      val path = "/Users/Di/Documents/ECE695/trace_201708/server_event.csv"
      val records: Dataset[ServerEvent] = readFile(path)
      val util = new Utils(spark, records)
      
      val distinctMachineIdNum = util.countUniqueMachineId()
      val result: RDD[ServerResults] = 
        spark.sparkContext.parallelize[ServerResults](Seq(ServerResults(distinctMachineIdNum)))
      result.saveAsTextFile("./output/")
      
  }

  def readFile(path: String): Dataset[ServerEvent] = {
    val records: DataFrame =
      spark
        .read
        .csv(path)
    
    
    import spark.implicits._
    
    records.map{
      r =>
        ServerEvent(r.getString(0).toInt, 
          r.getString(1).toInt, 
          r.getString(2),
          Some(r.getString(3)), 
          r.getString(4).toInt,
          r.getString(5).toFloat, 
          r.getString(6).toFloat)}
  }
}

object ServerJob {
  def main(args: Array[String]) {
    val serverEventPath = args(0)

    val spark: SparkSession = SparkSession.builder().appName("ServerEvent").getOrCreate()
    val job = new ServerJob(spark)
    val results = job.run(serverEventPath)
  }
}
