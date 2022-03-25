package HomeWork

import org.apache.spark.{SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Demo03 {
  def main(args: Array[String]): Unit = {
//    if (args.length != 2) {
//      println(
//        """
//          |      缺少参数！
//          |inputpath   outputpath
//          |""".stripMargin)
//      sys.exit()
//    }

    //创建SparkSession
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("Demo01").getOrCreate()
    var sc = spark.sparkContext

    //导入隐式转换
    import spark.implicits._

    //接收参数
    var Array(inputpath, outputpath) = args

    val stringToTuples: Map[String, List[(String, Long)]] = sc.textFile(inputpath).map(_.split(",", -1)).filter(_.length >= 85).map(arr => {
      //省
      var pro = arr(24)
      //市
      var city = arr(25)
      ((pro, city), 1)
    }).countByKey().groupBy(_._1._1).mapValues(x => {
      x.map(arr => {
        (arr._1._2, arr._2)
      }).toList.sortBy(_._2)
    }) // 得到每个省里市出现的次数
    val value: RDD[(String, List[(String, Long)])] = sc.makeRDD(stringToTuples.toList)

    //写出
    value.coalesce(1).saveAsTextFile(outputpath)

    //释放资源
    spark.stop()
    sc.stop()
  }
}
