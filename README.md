# ECE695

mvn compile

mvn assembly:single

spark-submit --class main.scala.com.ece.alianalysis.ServerJob --master local ./target/ ./output
