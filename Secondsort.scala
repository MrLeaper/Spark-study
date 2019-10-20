import org.apache.spark.{SparkConf, SparkContext}

object Secondsort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("11")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("test")
    val pairs = lines.map(line => {
      (new SecondKey(line.split(" ")(0).toInt,line.split(" ")(0).toInt),line)
    })
    val sortPairs = pairs.sortByKey()
    val sortline = sortPairs.map(line => line._2)
    sortline.foreach(s => println(s))
  }
}
