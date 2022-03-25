package com.dahua.analyse

import org.apache.spark.{HashPartitioner, SPARK_BRANCH, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProCityAnalyseForRDD {
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
    val line: RDD[String] = sc.textFile(inputpath)

    //对数据进行ETL,读元数据
    val ProCityRDD: RDD[((String, String), Int)] = line.map(_.split(",", -1)).filter(_.length >= 85).map(arr => {
      //得到省份
      var pro = arr(24)
      //得到市
      var city = arr(25)
      //把得到的数据放到元组种
      ((pro, city), 1)
    })

    //降维
    val reduceRDD: RDD[((String, String), Int)] = ProCityRDD.reduceByKey(_ + _)

    //把省份单独拿出来
    val rdd2: RDD[(String, (String, Int))] = reduceRDD.map(arr => {
      (arr._1._1, (arr._1._2, arr._2))
    })

    //得到省的个数
    val num: Long = rdd2.map(x => {
      (x._1, 1)
    }).reduceByKey(_ + _).count()

    //按照省的个数分区，并写出
    rdd2.partitionBy(new HashPartitioner(num.toInt)).coalesce(1).saveAsTextFile(outputpath)

    //释放资源
    spark.stop()
    sc.stop()
  }
}
