package com

import org.apache.spark.{SparkConf, SparkContext}

package object sparkdemo {
  val file = "/test/fans.txt"

  sealed trait Country { val country: String }
  case object Spain extends Country { val country = "spain" }
  case object Portugal extends Country { val country =  "portugal" }
  case object England  extends Country { val country = "england" }

  sealed trait Fan{ val country: String; val age: Int; }
  final case class SpainFan(age: Int) extends Fan { val country = Spain.country }
  final case class PortugalFan(age: Int) extends Fan { val country = Portugal.country }
  final case class EnglandFan(age: Int) extends Fan { val country = England.country }

  lazy val sc = {
    val conf = new SparkConf().setAppName("Spark demo")
    new SparkContext(conf)
  }

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
      .saveAsTextFile(file)
  }

  def stop(): Unit = {
    sc.stop()
  }
}
