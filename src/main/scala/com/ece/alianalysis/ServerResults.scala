package main.scala.com.ece.alianalysis
/**
  * Created by Di on 10/24/17.
  */


case class ServerResults (
  distinctServerEventMachineIdNum: Long,
  addEvent: EventCount,
  softErrorEvent: EventCount,
  agentCheckEvent: EventCount,
  machineFailEvent: EventCount,
  daemonCheckEvent: EventCount,
  diskFullEvent: EventCount,
  ncDisklessEvent: EventCount,
  cpuInfoEvent1: EventCount,
  cpuInfoEvent2: EventCount,
  memoryInfo: String,
  diskInfo: String                       
)

case class EventCount(
  name: String,
  count: Long                   
)

case class StatsInfo(
  name: String,                  
  count: Long,
  mean: Float,
  stddev: Float,
  min: Float,
  max: Float                  
)

