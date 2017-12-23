# ECE695

mvn compile

mvn assembly:single

spark-submit --class main.scala.com.ece.alianalysis.ServerJob --master local --jars ./target/scala-spark-2.2.0-SNAPSHOT-jar-with-dependencies.jar /Users/Di/Downloads/trace_201708/server_event.csv /Users/Di/Downloads/trace_201708/server_usage.csv ./output1/serverUsageRecords

spark-submit --jars ./target/scala-spark-2.2.0-SNAPSHOT-jar-with-dependencies.jar --class main.scala.com.ece.alianalysis.ContainerJob ./../data/container_usage.csv ./output1

spark-submit --jars ./target/scala-spark-2.2.0-SNAPSHOT-jar-with-dependencies.jar --class main.scala.com.ece.alianalysis.ContainerJob /Users/Di/Downloads/trace_201708/container_usage.csv /Users/Di/Downloads/trace_201708/container_event.csv ./output1