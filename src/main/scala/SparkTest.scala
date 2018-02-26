import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("spark-assignment1"))
    val threshold = 3
    val objectSales = new Sales
    objectSales.getSales(sc)
  }
}