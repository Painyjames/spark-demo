package com.sparkdemo

object Start extends App {
  load(10)
  val fansRDD = mapByCountry
  avgAgeByCountry(fansRDD)
  countByTicketType
  stop
}
