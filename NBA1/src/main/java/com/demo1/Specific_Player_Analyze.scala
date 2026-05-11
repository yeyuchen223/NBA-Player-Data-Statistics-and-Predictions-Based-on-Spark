package com.demo1

import org.apache.spark.sql.SaveMode

object Specific_Player_Analyze {
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder()
    .appName("Specific_Player_Analyze")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val filepath = "C:\\Users\\ctg80\\Desktop\\NBA1\\output7\\playerStatsZ"

    // 读取CSV文件
    val df = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv(filepath)

    df.createOrReplaceTempView("sp")

    // 查询指定球员的数据
    val score = spark.sql(
      """
        |SELECT
        |  `Player`, `year`, `Age`, `Tm`, `PTS`, `TRB`, `AST`, `STL`,
        |  `BLK`, `zTOT`, `nTOT`
        |FROM sp
        |WHERE Player = 'Kobe Bryant'
        |ORDER BY `year`
      """.stripMargin)

    // 保存结果
    val outputPath = "C:\\Users\\ctg80\\Desktop\\NBA1\\output6\\Kobe"
    score.write.mode(SaveMode.Overwrite).option("header", true).csv(outputPath)

    println("Specific Player Analyzed")

    spark.stop()
  }
}
