package test.scala.com.ece.alianalysis

import main.scala.com.ece.alianalysis.Analysis
import org.scalatest.{FlatSpec, Matchers}



class AnalysisTest extends FlatSpec with Matchers with SparkSessionTestWrapper{
  val utils = new TestUtils 
  var eventRecords = utils.createEventRecords()
  var usageRecords = utils.createUsageRecords()
  var analysis = new Analysis(spark, eventRecords, usageRecords)
  
  "countUniqueServerEventMachineId" should "count distinct machine id in server event" in {
      val eventCount = analysis.countUniqueServerEventMachineId()
      assert(eventCount === 4)
  }

  "countUniqueServerUsageMachineId" should "count distinct machine id in server usage" in {
    val usageCount = analysis.countUniqueServerUsageMachineId()
    assert(usageCount === 10)
  }

  "serverEventType" should "count the frequency of each event type in server event" in {
    val serverEventType = analysis.serverEventType()
    assert(serverEventType.count() === 2)
    assert(serverEventType.filter( e=> e._1 == "add").first()._2 === 2)
    assert(serverEventType.filter( e=> e._1 == "softerror").first()._2 === 2)
  }
  
  "serverDetailedEventType" should "count the frequency of each detailed event in server event" in {
    val detailedServerEventType = analysis.serverDetailedEventType()
    detailedServerEventType.count() == 2
    assert(detailedServerEventType.filter(e=>e._1 == "agent_check").first()._2 === 1)
    assert(detailedServerEventType.filter(e=>e._1 == "machine_fail").first()._2 === 1)
  }
  
  "cpuCapacity" should "count the capacity for cpu" in {
    val cpuInfo = analysis.cpuCapacity()
    assert(cpuInfo.count() === 2)
    assert(cpuInfo.filter(e => e._1 == 64).first()._2 === 3)
    assert(cpuInfo.filter(e => e._1 == 0).first()._2 === 1)
  }
  
  "memoryInfo" should "calculate memory information" in {
    val memInfo = analysis.memoryInfo().toString
    memInfo shouldBe "StatsInfo(Memory Info,4,0.51750445,0.34500298,0.0,0.69000596)"
  }
  
  "diskInfo" should "calculate disk information" in {
    val diskInfo = analysis.diskInfo().toString
    diskInfo shouldBe "StatsInfo(disk Info,4,0.75,0.5,0.0,1.0)"
  }
  
  "numOfMachinesAtSameTime" should "calculate the number of machines used at the same time" in {
    val numOfMachinesAtSameTime = analysis.numOfMachinesAtSameTime()
    assert(numOfMachinesAtSameTime.count() === 6)
    assert(numOfMachinesAtSameTime.filter(e=>e._1 == 41700).first()._2 === 1)
    assert(numOfMachinesAtSameTime.filter(e=>e._1 == 39600).first()._2 === 4)
    assert(numOfMachinesAtSameTime.filter(e=>e._1 == 42600).first()._2 === 1)
    assert(numOfMachinesAtSameTime.filter(e=>e._1 == 40800).first()._2 === 1)
    assert(numOfMachinesAtSameTime.filter(e=>e._1 == 42900).first()._2 === 1)
    assert(numOfMachinesAtSameTime.filter(e=>e._1 == 40200).first()._2 === 2)
  }
  
}
