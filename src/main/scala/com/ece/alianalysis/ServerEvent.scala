/**
  * Created by Di on 10/23/17.
  */
package main.scala.com.ece.alianalysis

case class ServerEvent (
    timeStamp: Integer,
    machineId: Integer,
    eventType: String,
    eventDetail: Option[String],
    numberOfCpus: Integer,
    normalizedMemory: Float,
    normalizedDiskSpace: Float                  
)

case class ServerUsage(
    timeStamp: Integer,
    machineId: Integer,
    usedPercentOfCpus: Float,
    usedPercentOfMemory: Float,
    usedPercentOfDiskSpace: Float,
    linuxCpuLoadAvergOfOneMin: Float,
    linuxCpuLoadAvergOfFiveMin: Float,
    linuxCpuLoadAvergOfFifteenMin: Float                  
)