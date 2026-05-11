package com.demo1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Age_Exp_Thrend_Analyze {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }

    val spark = SparkSession.builder()
      .appName("AgeAndExpThrendAnalysis")
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    val yearRawDataPath = "C:\\Users\\ctg80\\Desktop\\NBA1\\output"

    val df = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv(yearRawDataPath)

    // 添加经验字段
    val playerWithExp = df.withColumn("experience", $"year" - $"Age" + 1)

    playerWithExp.createOrReplaceTempView("playerStats")

    // 计算年龄和经验的趋势分析
    val ageTrend = spark.sql(
      """
      SELECT Age as ageOrExp,
             COUNT(*) as value_count,
             AVG(PTS) as valuePTS_mean,
             STDDEV(PTS) as valuePTS_stddev,
             MAX(PTS) as valuePTS_max,
             MIN(PTS) as valuePTS_min,
             AVG(TRB) as valueTRB_mean,
             STDDEV(TRB) as valueTRB_stddev,
             MAX(TRB) as valueTRB_max,
             MIN(TRB) as valueTRB_min,
             AVG(AST) as valueAST_mean,
             STDDEV(AST) as valueAST_stddev,
             MAX(AST) as valueAST_max,
             MIN(AST) as valueAST_min
      FROM playerStats
      GROUP BY Age
      ORDER BY Age
    """)

    ageTrend.write.option("header", true).mode(SaveMode.Overwrite).csv("C:\\Users\\ctg80\\Desktop\\NBA1\\output2\\AgeTrend")
    println("Age Trend Analyzed")

    val expTrend = spark.sql(
      """
      SELECT experience as ageOrExp,
             COUNT(*) as value_count,
             AVG(PTS) as valuePTS_mean,
             STDDEV(PTS) as valuePTS_stddev,
             MAX(PTS) as valuePTS_max,
             MIN(PTS) as valuePTS_min,
             AVG(TRB) as valueTRB_mean,
             STDDEV(TRB) as valueTRB_stddev,
             MAX(TRB) as valueTRB_max,
             MIN(TRB) as valueTRB_min,
             AVG(AST) as valueAST_mean,
             STDDEV(AST) as valueAST_stddev,
             MAX(AST) as valueAST_max,
             MIN(AST) as valueAST_min
      FROM playerStats
      GROUP BY experience
      ORDER BY experience
    """)

    expTrend.write.option("header", true).mode(SaveMode.Overwrite).csv("C:\\Users\\ctg80\\Desktop\\NBA1\\output2\\ExpTrend")
    println("Exp Trend Analyzed")

    spark.stop()
  }
}
