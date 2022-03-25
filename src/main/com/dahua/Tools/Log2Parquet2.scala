package com.dahua.Tools

import com.dahua.Bean.LogBean
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Log2Parquet2 {
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

    // 将自定义类注册kryo序列化。
    conf.registerKryoClasses(Array(classOf[LogBean]))

    val spark = SparkSession.builder().config(conf).appName("Log02Parquet").master("local[1]").getOrCreate()
    var sc = spark.sparkContext

    //导入隐式转换
    import spark.implicits._

    //接收参数  读取原数据。
    var Array(inputpath, outputpath) = args

    // 对数据进行ETL
    val rdd: RDD[Array[String]] = sc.textFile(inputpath).map(_.split(",", -1)).filter(_.length >= 85)
    val rddLogBean: RDD[LogBean] = rdd.map(LogBean(_))

    //将RDD转换为DF
    val df: DataFrame = spark.createDataFrame(rddLogBean)

    // 写出到parquet
    df.write.parquet(outputpath)

    spark.stop()
    sc.stop()
  }
}
