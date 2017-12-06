/**
  * Created by Di on 11/28/17.
  */

package main.scala.com.ece.alianalysis

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
