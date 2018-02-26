import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("spark-assignment1").setMaster("local[*]"))
    val threshold = 3
    val objectSales = new Sales
    objectSales.getSales(sc)
    Thread.sleep(10000)
  }
}