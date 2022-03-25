package com.dahua.Utils

object NumFormat {

  //转化成int类型
  def toInt(data: String): Int = {
    try {
      data.toInt
    } catch {
      case _: Exception => 0
    }
  }

  //转化成double类型的数据
  def toDouble(data: String): Double = {
    try {
      data.toDouble
    } catch {
      case _: Exception => 0
    }
  }
}
