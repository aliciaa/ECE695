/**
  * Created by Di on 12/15/17.
  */

package main.scala.com.ece.alianalysis
case class ContainerEvent (
                            timeStamp: Integer,
                            eventType: String,
                            instanceId: Long,
                            machineId: Long,
                            plan_cpu: Integer,
                            plan_mem: Float,
                            plan_disk: Float,
                            cpuSet: Seq[Integer]
                          )
