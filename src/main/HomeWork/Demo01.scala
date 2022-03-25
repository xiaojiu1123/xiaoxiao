package HomeWork

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Demo01 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |      缺少参数！
          |inputpath   outputpath
          |""".stripMargin)
      sys.exit()
    }

    //创建SparkSession
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("Demo01").master("local[1]").getOrCreate()
    var sc = spark.sparkContext

    //导入隐式转换
    import spark.implicits._

    //接收参数
    var Array(inputpath, outputpath) = args

    //读取数据
    val line: RDD[String] = sc.textFile(inputpath)

    val value: RDD[(String, Int)] = line.map(_.split(",", -1)).filter(_.length >= 85).map(arr => {
      //得到省份
      var pro = arr(24)
      //得到市
      var city = arr(25)
      //把得到的数据放到元组种
      (pro, 1)
    }).groupBy(_._1).map(x => {
      (x._1, x._2.size)
    }).coalesce(1).sortBy(_._2)

    //写出   Demo01_OutPut
    value.saveAsTextFile(outputpath)

    //释放资源
    spark.stop()
    sc.stop()
  }
}
