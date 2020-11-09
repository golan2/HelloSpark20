package golan.hello.spark.core

import org.apache.spark.sql.SparkSession

class ReduceSquareRoot {}
object ReduceSquareRoot {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ReduceSquareRoot")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._

    def sqrtOfSumOfSq(x: Double, y: Double) = {
      val res = math.sqrt(x*x+y*y)
      println(s"($x , $y) ===> $res")
      res
    }

    val ds = spark.range(1, 10, 1).map(ll => ll.doubleValue())
    ds.repartition(3)

    val sumOfSquars: Double = ds.reduce((x, y) => sqrtOfSumOfSq(x, y))

    println(sumOfSquars)

  }

}
