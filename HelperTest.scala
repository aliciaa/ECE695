import org.scalatest.junit.AssertionsForJUnit
import org.apache.spark.SparkContext
import org.junit.Test
import org.junit.Before
import org.apache.spark.SparkConf
import org.junit.After

class HelperTest extends AssertionsForJUnit {
  var sc: SparkContext = _
  var job: Helper = _
  @Before
  def initialize() {
    val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
    sc = new SparkContext(conf)

    job = new Helper(sc)
  }

  @After
  def tearDown() {
    sc.stop()
  }

  @Test
  def testCreateTestData() {
    val cols = new Array[Int](2)
    cols(0) = 0
    cols(1) = 1
    val numRows = 1000
    val result = job.createTestData("trace_201708/batch_instance.csv", "batch_instance_TestData", cols, numRows)
  }



  @Test
  def testGetGroupMax(): Unit = {
    val arr = Array(0,2)
    val res = job.getGroupMax("testFile.csv", arr)
    res.collect().foreach(t =>
      if(t._1 == 1) {
        assert(t._2 === 3)
      }else if(t._1 == 2) {
        assert(t._2 === 2)
      }else {
        assert(t._2 === 0)
      })
    //res.collect().foreach(println)
  }

  @Test
  def testGetGroupMin(): Unit = {
    val arr = Array(0,2)
    val res = job.getGroupMin("testFile.csv", arr)
    res.collect().foreach(t =>
      if(t._1 == 1) {
        assert(t._2 === 0)
      }else if(t._1 == 2) {
        assert(t._2 === 1)
      }else {
        assert(t._2 === 0)
      })
    res.collect().foreach(println)
  }

  @Test
  def testGetTaskDuration(): Unit = {
    val arrStart = Array(0,2)
    val arrEnd = Array(0,3)
    val jobStart = job.getGroupMin("testFile.csv", arrStart)
    val jobEnd = job.getGroupMax("testFile.csv", arrEnd)
    val res = job.getTaskDuration(jobStart, jobEnd)
    res.collect().foreach(println)
  }


}
