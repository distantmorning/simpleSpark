package cn.hadron
import org.apache.spark._
object WordCount {
  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    var inputPath = "D:\\data\\ehdata"
    var outputPath = "D:\\output"
    if (args.length == 1) {
      masterUrl = args(0)
    } else if (args.length == 3) {
      masterUrl = args(0)
      inputPath = args(1)
      outputPath = args(2)
    }
    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rowRdd = sc.textFile(inputPath)
    val resultRdd = rowRdd.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    resultRdd.saveAsTextFile(outputPath)
  }
}
