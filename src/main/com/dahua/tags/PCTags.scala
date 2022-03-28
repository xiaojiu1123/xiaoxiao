package com.dahua.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object PCTags extends TraitTag {
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String, Int]()
    val row: Row = args(0).asInstanceOf[Row]
    val provinceName: String = row.getAs[String]("provincename")
    val cityName: String = row.getAs[String]("cityname")
    if (StringUtils.isNotEmpty("provinceName")) map += "ZP" + provinceName -> 1
    if (StringUtils.isNotEmpty("cityname")) map += "ZP" + cityName -> 1
    map
  }

}
