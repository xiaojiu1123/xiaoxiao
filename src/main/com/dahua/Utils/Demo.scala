package com.dahua.Utils

import java.util.Random

object Demo {

  //随机数
  def main(args: Array[String]): Unit = {
    var random = new Random()
    val i: Int = random.nextInt(20)
    println(i)
  }


}
