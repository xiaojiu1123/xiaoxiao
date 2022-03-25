package com.dahua.Dim

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ZoneDimForRDD {
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
    val spark = SparkSession.builder().config(conf).appName("Log02Parquet").master("local[1]").getOrCreate()
    var sc = spark.sparkContext

    //导入隐式转换
    import spark.implicits._

    //接收参数
    var Array(inputpath, outputpath) = args

    //读取数据
    val df: DataFrame = spark.read.parquet(inputpath)

    //获取字段
    val dimRDD: Dataset[((String, String), List[Double])] = df.map(row => {
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val province: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")

      //降维写到方法种

      val qqs: List[Double] = Dimzhibiao.qqsRtp(requestmode, processnode)
      val jingjia: List[Double] = Dimzhibiao.jjsRtp(adplatformproviderid, iseffective, isbilling, isbid, iswin, adorderid)
      val zss: List[Double] = Dimzhibiao.zssRtp(requestmode, iseffective)
      val mjzss: List[Double] = Dimzhibiao.mjzssRtp(requestmode, iseffective, isbilling)
      val chengben: List[Double] = Dimzhibiao.xiaofei(adplatformproviderid, iseffective, isbilling, iswin, adorderid, adplatformproviderid, winprice, adpayment)
      ((province, cityname), qqs ++ jingjia ++ zss ++ mjzss ++ chengben)
    })

    //聚合
    dimRDD.rdd.reduceByKey((list1,list2) => {
      list1.zip(list2).map(x => x._1+x._2)
    }).foreach(println(_))
  }
}
