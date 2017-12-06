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

