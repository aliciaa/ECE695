package main.scala.com.ece.alianalysis

import org.apache.hadoop.yarn.util.Records
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

/**
  * Created by Di on 10/24/17.
  */
class Utils(spark: SparkSession) extends java.io.Serializable {
  
    def toInt(input: String): Integer ={
        import spark.implicits._
        try {
          input.toInt
        } catch {
          case e: Exception => -1
        }
    }
  
    def toFloat(input: String): Float = {
        import spark.implicits._
      try {
        input.toFloat
      } catch {
        case e: Exception => -1.0f
      }
    }
}
