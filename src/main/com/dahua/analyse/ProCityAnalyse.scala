package com.dahua.analyse

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityAnalyse {
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
    var conf = new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("Log02Parquet").master("local[1]").getOrCreate()
    var sc = spark.sparkContext

    //导入隐式转换
    import spark.implicits._

    //接收参数
    var Array(inputpath,outputpath) = args

    //读取数据 ，读OutPut1
    val df: DataFrame = spark.read.parquet(inputpath)

    //将数据映射成一个临时表
    df.createTempView("log")

    //编写sql语句
    var sql = "select provincename,cityname,count(*) as pccount from log group by provincename ,cityname "

    //执行sql语句
    val resDF: DataFrame = spark.sql(sql)


    val configuration: Configuration = sc.hadoopConfiguration

    //文件系统对象
    val fs: FileSystem = FileSystem.get(configuration)

    //创建Path路径对象
    var path = new Path(outputpath)
    //如果路径存在的话，删除，不存在的话写出
    if(fs.exists(path)){
      fs.delete(path,true)
    }

    //PCOutPut1
    resDF.coalesce(1).write.json(outputpath)

    spark.stop()
    sc.stop()
  }

}
