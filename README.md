# ECE695

mvn compile

mvn assembly:single

spark-submit --class main.scala.com.ece.alianalysis.ServerJob --master local ./target/scala-spark-2.2.0-SNAPSHOT-jar-with-dependencies.jar /Users/Di/Downloads/trace_201708/server_event.csv /Users/Di/Downloads/trace_201708/server_usage.csv ./output1
