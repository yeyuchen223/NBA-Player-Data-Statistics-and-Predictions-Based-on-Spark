package com.demo1

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object Player_Stats_Preprocess {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").set("spark.eventLog.enabled", "false")

    val spark = SparkSession.builder()
      .appName("PlayerStatsPreprocess")
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val rawDataPath = "C:\\Users\\ctg80\\Desktop\\NBA1\\data\\basketball"
    val hdfspath = "C:\\Users\\ctg80\\Desktop\\NBA1\\output"



    val rdds: Seq[RDD[(Int, String)]] = (1996 to 2023).map { year =>
      val nextYearSuffix = (year + 1).toString.substring(2)
      sc.textFile(s"${rawDataPath}/leagues_NBA_${year}-${nextYearSuffix}_per_game_per_game.csv").filter(line =>
        !line.trim.isEmpty && !line.startsWith("Rk")).map(line => {
        val temp = line.replaceAll("\\*", "")
          .replaceAll(",,", ",0,")
          .replaceAll(",,", ",0,")
        (year, s"${year},${temp}")
      })
    }





    import spark.implicits._
    val processedDF = rdds.reduce(_.union(_)).toDF("year", "line")

    // Split the "line" column into separate columns based on the original data structure
    val columns = Seq("year", "Rk", "Player", "Pos", "Age", "Tm", "G", "GS", "MP", "FG", "FGA", "FG%", "3P", "3PA", "3P%", "2P", "2PA", "2P%", "eFG%", "FT", "FTA", "FT%", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS")
    val finalDF = processedDF.selectExpr(
      "split(line, ',')[0] as year",
      "split(line, ',')[1] as Rk",
      "split(line, ',')[2] as Player",
      "split(line, ',')[3] as Pos",
      "split(line, ',')[4] as Age",
      "split(line, ',')[5] as Tm",
      "split(line, ',')[6] as G",
      "split(line, ',')[7] as GS",
      "split(line, ',')[8] as MP",
      "split(line, ',')[9] as FG",
      "split(line, ',')[10] as FGA",
      "split(line, ',')[11] as `FG%`",
      "split(line, ',')[12] as `3P`",
      "split(line, ',')[13] as `3PA`",
      "split(line, ',')[14] as `3P%`",
      "split(line, ',')[15] as `2P`",
      "split(line, ',')[16] as `2PA`",
      "split(line, ',')[17] as `2P%`",
      "split(line, ',')[18] as `eFG%`",
      "split(line, ',')[19] as FT",
      "split(line, ',')[20] as FTA",
      "split(line, ',')[21] as `FT%`",
      "split(line, ',')[22] as ORB",
      "split(line, ',')[23] as DRB",
      "split(line, ',')[24] as TRB",
      "split(line, ',')[25] as AST",
      "split(line, ',')[26] as STL",
      "split(line, ',')[27] as BLK",
      "split(line, ',')[28] as TOV",
      "split(line, ',')[29] as PF",
      "split(line, ',')[30] as PTS"
    )

    finalDF.write
      .mode(SaveMode.Overwrite)
      .option("header", "true") // Ensure the header is included
      .partitionBy("year")
      .csv(hdfspath)

    println("Player Stats Processed")
    spark.stop()
  }
}
