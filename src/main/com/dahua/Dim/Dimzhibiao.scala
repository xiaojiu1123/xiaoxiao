package com.dahua.Dim

object Dimzhibiao {
  //请求数
  def qqsRtp(requestmode : Int,processnode : Int): List[Double] ={
    if(requestmode ==1 && processnode >=1) {
      List[Double](1, 0, 0)
    }else if(requestmode == 1 && processnode >=2) {
      List[Double](1,1,0)
    }else if(requestmode == 1 && processnode >= 3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }

  //参与竞价数
  def jjsRtp(adplatformproviderid : Int,iseffective : Int,isbilling : Int,isbid :Int,iswin : Int,adorderid : Int): List[Double] ={
      if(adplatformproviderid >= 100000 && iseffective ==1 && isbilling == 1 && isbid == 1 && adorderid != 0){
        List[Double](1,0)
      }else if(adplatformproviderid >= 100000 && iseffective == 1 && isbilling == 1 && iswin == 1){
        List[Double](1,1)
      }else{
        List[Double](0,0)
      }
  }

  //广告展示数与点击数
  def zssRtp(requestmode : Int,iseffective : Int): List[Double] ={
    if(requestmode == 2 && iseffective == 1){
      List[Double](1,0)
    }else if(requestmode == 3 && iseffective == 1){
      List[Double](1,1)
    }else{
      List[Double](0,0)
    }
  }

  //媒介展示数和点击量
  def mjzssRtp(requestmode : Int,iseffective : Int,isbilling : Int): List[Double] ={
    if(requestmode == 2 && iseffective == 1 && isbilling == 1){
      List[Double](1,0)
    }else if(requestmode == 3 && iseffective == 1 && isbilling == 1){
      List[Double](1,1)
    }else{
      List[Double](0,0)
    }
  }

  //广告消费和成本
  def xiaofei(adplatformproviderid : Int,iseffective : Int,isbilling : Int,iswin : Int,adorderid : Int,adcreativeid : Int,winprice : Double,adpayment : Double): List[Double] ={
      if(adplatformproviderid >= 100000 && iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid > 200000 && adcreativeid >200000){
        List[Double](winprice*1.0/1000,adpayment*1.0/1000)
      }else{
        List[Double](0,0)
      }
  }

}
