package test.scala.com.ece.alianalysis

import main.scala.com.ece.alianalysis.{ServerEvent, ServerUsage}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Di on 11/3/17.
  */

trait SparkSessionTestWrapper{
  val spark : SparkSession = 
    SparkSession
      .builder
      .master("local")
      .appName("ServerEvent")
      .getOrCreate()
  
}

class TestUtils extends SparkSessionTestWrapper{
  
    import spark.implicits._
    def createEventRecords() : Dataset[ServerEvent] =  {
        val r1 = new ServerEvent(100, 11, "add", Some(""), 64, 0.69000595f, 1.0f)
        val r2 = new ServerEvent(101, 12, "add", Some(""), 64, 0.69000595f, 1.0f)
        val r3 = new ServerEvent(102, 13, "softerror", Some("agent_check"), 64, 0.69000595f, 1.0f)
        val r4 = new ServerEvent(103, 14, "softerror", Some("machine_fail"), 0, 0.0f, 0.0f)
        spark
          .sparkContext
          .parallelize[ServerEvent](Seq(r1, r2, r3, r4))
          .toDS()
    }
  
    def createUsageRecords() : Dataset[ServerUsage] = {
        val r1 = new ServerUsage(41700,237,23.3799999237f,30.08000030518f,42.2000007629f,15.820000267020001f,13.860000038159999f,12.64000015258f)
        val r2 = new ServerUsage(39600,265,26.3599998474f,29.539999771139996f,57.59999847409999f,17.46000022886f,18.9f,16.70000038146f)
        val r3 = new ServerUsage(42600,770,49.14000015258f,60.099999237060004f,41.8600013733f,33.20000000002f,31.2200000763f,30.51999969482f) 
        val r4 = new ServerUsage(40800,776,33.23999977112f,47.52000045776f,43.5999984741f,21.83999977112f,22.10000038146f,24.02000045776f)
        val r5 = new ServerUsage(42900,393,45.72000045776f,58.720000457779996f,42.0f,34.10000038148f,36.2399986267f,36.91999969482f)
        val r6 = new ServerUsage(39600,610,41.7f,59.22000122072001f,42.5999984741f,29.85999984742f,29.399999999979997f,25.50000038146f)
        val r7 = new ServerUsage(39600,719,43.57999954222f,63.30000076294f,41.72000045774f,32.0200000763f,31.479999542220003f,27.160000228860003f)
        val r8 = new ServerUsage(39600,723,42.48000030518f,66.2f,41.9000015259f,31.01999969484f,31.5f,27.079999542259998f)
        val r9 = new ServerUsage(40200,742,35.90000000002f,57.70000076294f,41.7000007629f,23.01999969484f,25.2200000763f,26.520000076320002f)
        val r10 = new ServerUsage(40200,22,45.7f,61.8800010681f,41.2999992371f,33.840000534059996f,37.12000045776f,37.23999938964f)
        spark
          .sparkContext
          .parallelize[ServerUsage](Seq(r1, r2, r3, r4, r5, r6, r7, r8, r9, r10))
          .toDS()
    }
}
