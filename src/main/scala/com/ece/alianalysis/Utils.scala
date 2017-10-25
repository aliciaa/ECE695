package main.scala.com.ece.alianalysis

import org.apache.hadoop.yarn.util.Records
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Di on 10/24/17.
  */
class Utils(spark: SparkSession, records: Dataset[ServerEvent]) {
  
    def countUniqueMachineId(): Long ={
        import spark.implicits._
        records
          .map(t=>t.machineId)
          .distinct()
          .count()
    }
}
