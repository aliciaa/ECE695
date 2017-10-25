import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map


class Helper(sc: SparkContext) {

  def createTestData(realDataPath: String, testDataFolderPath: String, colIndexArr: Array[Int], numRows: Int): Unit = {
    val realData = sc.textFile(realDataPath)
    val part_of_my_rdd = sc.parallelize(realData.take(numRows))
    part_of_my_rdd.saveAsTextFile(testDataFolderPath);
  }

  def getGroupMax(dataPath: String, colIndexArr: Array[Int]): RDD[(Int,Int)] = {
    val dataRDD = sc.textFile(dataPath)
      .map(x => (x.split(",")(colIndexArr(0)).toInt, x.split(",")(colIndexArr(1)).toInt))
    val groupMaxPairs = dataRDD.reduceByKey(math.max(_,_))
    return groupMaxPairs
  }

  def getGroupMin(dataPath: String, colIndexArr: Array[Int]): RDD[(Int,Int)] = {
    val dataRDD = sc.textFile(dataPath)
      .map(x => (x.split(",")(colIndexArr(0)).toInt, x.split(",")(colIndexArr(1)).toInt))
    val groupMaxPairs = dataRDD.reduceByKey(math.min(_,_))
    return groupMaxPairs
  }

  def getTaskDuration(jobStart: RDD[(Int,Int)], jobEnd: RDD[(Int,Int)]): RDD[(Int,Option[Int])] = {
    val jn = jobStart.leftOuterJoin(jobEnd).values
    return jn
  }
}
