package com.demo1

import org.apache.spark.sql.SaveMode

object Basic_Stats_Analyze {
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  val filepath = "C:\\Users\\ctg80\\Desktop\\NBA1\\output\\"

  def main(args: Array[String]): Unit = {
    // read the csv
    val df = spark.read.options(
      Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv(filepath)

    df.show(false)

    df.createOrReplaceTempView("zdf")
    val score = spark.sql("select `Player` as name, `year`, `Tm` as team, `PTS` from zdf where `G` > 30 ")
    score.show(false)

    score.write.mode(SaveMode.Overwrite).option("header", true).csv("C:\\Users\\ctg80\\Desktop\\NBA1\\output1\\basic\\score")

    val assists = spark.sql("select `Player` as name, `year`, `Tm` as team, `AST` from zdf where `G` > 30 ")
    assists.write.mode(SaveMode.Overwrite).option("header", true).csv("C:\\Users\\ctg80\\Desktop\\NBA1\\output1\\basic\\assists")

    val rebound = spark.sql("select `Player` as name, `year`, `Tm` as team, `TRB` from zdf where `G` > 30 ")
    rebound.write.mode(SaveMode.Overwrite).option("header", true).csv("C:\\Users\\ctg80\\Desktop\\NBA1\\output1\\basic\\rebound")

    val steal = spark.sql("select `Player` as name, `year`, `Tm` as team, `STL` from zdf where `G` > 30 ")
    steal.write.mode(SaveMode.Overwrite).option("header", true).csv("C:\\Users\\ctg80\\Desktop\\NBA1\\output1\\basic\\steal")

    val block = spark.sql("select `Player` as name, `year`, `Tm` as team, `BLK` from zdf where `G` > 30 ")
    block.write.mode(SaveMode.Overwrite).option("header", true).csv("C:\\Users\\ctg80\\Desktop\\NBA1\\output1\\basic\\block")

    println("Basic Stats Analyzed")
  }
}
