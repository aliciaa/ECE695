/**
  * Created by Di on 12/5/17.
  */
package main.scala.com.ece.alianalysis

case class ContainerUsage (
    startTime: Integer,
    onlineInstanceId: Integer,
    usedPercentOfRequestedCpus: Float,
    usedPercentOfRequestedMemory: Float,
    usedPercentOfRequestedDiskSpace: Float,
    linuxCpuLoadAverageOfOneMin: Float,
    linuxCpuLoadAverageOfFiveMin: Float,
    linuxCpuLoadAverageOfFifteenMin: Float,
    averagedCyclesPerInstruction: Float,
    averageCacheMissesPer1000Instructions: Float,
    maxCyclesPerInstruction: Float,
    maxCacheMissesPer1000Instructions: Float                      
)
