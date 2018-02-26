
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.joda.time.DateTime
class Sales {

  def getSales(sc: SparkContext): Unit = {
    val log = Logger.getLogger(this.getClass)
    val file1 = sc.textFile("file1")
    val file1Data = file1.map(line => (line.split('#')(0),line.split('#').toList))

    val file2 = sc.textFile("file2")
    val file2Data = file2.map(line => (line.split('#')(1),line.split('#').toList))

    val joined = file1Data.join(file2Data)
    joined.foreach(print)

    val data = joined.groupBy( state => state._2._1(4))

    val finalYear = data.flatMap(x=> x._2.groupBy(y=>new DateTime((y._2._2(0)).toLong * 1000).toDateTime.toString("yyyy")))

    val sumYear = finalYear.map(x=> (x._2.map(t=>t._2._1(4).distinct),x._1,x._2.map(y=> y._2._2(2).toInt).sum))


    val finalMonth = data.flatMap(x=> x._2.groupBy(y=>new DateTime((y._2._2(0)).toLong * 1000).toDateTime.toString("yyyy/MM")))

    val sumMonth = finalMonth.map(x=> (x._2.map(t=>t._2._1(4).distinct),x._1,x._2.map(y=> y._2._2(2).toInt).sum))

    val finalDate = data.flatMap(x=> x._2.groupBy(y=>new DateTime((y._2._2(0)).toLong * 1000).toDateTime.toString("yyyy/MM/dd")))

    val sumDate = finalDate.map(x=> (x._2.map(t=>t._2._1(4).distinct),x._1,x._2.map(y=> y._2._2(2).toInt).sum))

    val dataTemp = sumYear.union(sumMonth)

    val finalData = dataTemp.union(sumDate)

    finalData.saveAsTextFile("Result")

    log.info(s"\n\n\n\n\n\n\n\n\n${finalData.collect.toList}\n\n\n\n\n\n\n\n\n\n")




  }

}
