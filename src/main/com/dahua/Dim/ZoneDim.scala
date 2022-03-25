package com.dahua.Dim

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object ZoneDim {
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

    //创建视图临时表
    val log: Unit = df.createTempView("log")

    //编写sql语句
    var sql =
      """
        |select
        |provincename,cityname,
        |sum(case when requestmode =1 and processnode >=1 then 1 else 0 end) as ysqq,  --原始请求
        |sum(case when requestmode =1 and processnode >=2 then 1 else 0 end) as yxqq,  --有效请求
        |sum(case when requestmode =1 and processnode =3 then 1 else 0 end) as ggqq,  --广告请求
        |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) as cyjjs,  --参与竞价数
        |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling=1 and iswin=1 then 1 else 0 end) as jjcgs,  --竞价成功数
        |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as zss,  --展示量（广告主）
        |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as djs,  --点击量（广告主）
        |sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss,  --展示量（媒介）
        |sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs,  --点击量（媒介）
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as xiaofei,  --DSP广告消费
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as chengben  --DSP广告成本
        |from log
        |group by  --按照省市分组
        |provincename,cityname
        |""".stripMargin

    //运行sql语句
    val resDF: DataFrame = spark.sql(sql)

    val load: Config = ConfigFactory.load()

    //连接JDBC，连接数据库
    val peo = new Properties()
    peo.setProperty("user",load.getString("jdbc.user"))
    peo.setProperty("driver",load.getString("jdbc.driver"))
    peo.setProperty("password",load.getString("jdbc.password"))

    //写入到数据库
    resDF.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName2"),peo)

    //展示数据
   // resDF.show()

    //释放资源
    spark.stop()
    sc.stop()
  }
}
