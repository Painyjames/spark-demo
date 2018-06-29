package com

import java.time.Instant

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

package object sparkdemo {
  val timestamp = Instant.now.getEpochSecond
  val fansFile = s"/test/fans-${timestamp}.txt"
  val fansPerTicketTypeFile = s"/test/fansPerTicketType-${timestamp}.txt"
  val fansPerCountryFile = s"/test/fansPerCountry-${timestamp}.txt"

  sealed trait Country { val country: String }
  case object Spain extends Country { val country = "spain" }
  case object Portugal extends Country { val country =  "portugal" }
  case object England  extends Country { val country = "england" }

  sealed trait Fan{ val country: String; val age: Int; }
  final case class SpainFan(age: Int) extends Fan { val country = Spain.country }
  final case class PortugalFan(age: Int) extends Fan { val country = Portugal.country }
  final case class EnglandFan(age: Int) extends Fan { val country = England.country }

  lazy val conf = new SparkConf()
    .setAppName("Spark demo")

  lazy val sc = new SparkContext(conf)

  private[com] def generateFan(age: Int): Fan = {
    scala.util.Random.nextInt(3) match {
      case 0 => SpainFan(age)
      case 1 => PortugalFan(age)
      case 2 => EnglandFan(age)
    }
  }

  private[com] def generateAge(): Int = {
    scala.util.Random.nextInt(120)
  }

  def load(n: Int): Unit = {
    val fans = (0 to n).map(_ => generateFan(generateAge))
    sc.parallelize(fans)
      .map(f => s"${f.country},${f.age}")
      .saveAsTextFile(fansFile)
  }

  sealed trait TicketType { val value: String; }
  case object Kid extends TicketType { val value = "kid"; def apply() = value; }
  case object Adult extends TicketType { val value = "adult"; def apply() = value; }
  private[com] def mapTicketType(age: Int): String = {
    if(age < 18)
      Kid()
    else
      Adult()
  }

  case class Ticket(country: String, age: Int, ticketType: String)
  type T = (String, Int, String)
  def mapByCountry(): RDD[(String, Iterable[T])] = {
    val fansRDD = sc.textFile(fansFile)
    fansRDD.flatMap[(String, T)](x => x.split(",") match {
      case Array(country, ageS) => {
        val age = ageS.toInt
        Some((country, (country, age.toInt, mapTicketType(age))))
      }
      case _ => None
    }).groupByKey
  }

  type CountResult = (String, Int)
  def countByTicketType: Unit = {
    val fansView = "fansView"
    val fansDF = sc.textFile(fansFile)
      .map(f => {
        val Array(_, ageS) = f.split(",")
        val age = ageS.trim.toInt
        val ticketType = mapTicketType(age)
        (ticketType, 1)
      })
      .reduceByKey(_ + _)
      .saveAsTextFile(fansPerTicketTypeFile)
  }

  def avgAgeByCountry(fans: RDD[(String, Iterable[T])]): Unit = {
    fans.mapValues{ case it => it.foldLeft(0){ case (acc, (_, age, _)) => acc + age } / it.size }
      .saveAsTextFile(fansPerCountryFile)
  }

  def stop(): Unit = {
    sc.stop()
  }
}
