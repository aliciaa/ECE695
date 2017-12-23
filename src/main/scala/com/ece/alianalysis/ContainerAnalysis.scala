/**
  * Created by Di on 12/5/17.
  */
package main.scala.com.ece.alianalysis

import org.apache.spark.sql.{Dataset, SparkSession}

class ContainerAnalysis(spark: SparkSession, usageRecords: Dataset[ContainerUsage]) {
  //return value is time, # of active machines
  def numOfMachinesAtSameTime(): Dataset[(Integer, Int)] = {
    import spark.implicits._

    usageRecords
      .groupByKey(_.startTime)
      .mapGroups{
        case (time, records) => (time, records.map(r=>r.onlineInstanceId).toSeq.distinct.length)
      }
  }
  
}

