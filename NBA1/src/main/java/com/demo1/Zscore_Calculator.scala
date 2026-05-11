import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.log4j.{Level, Logger}

object ZscoreCalculator {
  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 配置Spark
    val conf = new SparkConf().setAppName("ZscoreCalculator").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 读取数据
    val inputPath = "C:\\Users\\ctg80\\Desktop\\NBA1\\output"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)

    // 创建临时视图
    df.createOrReplaceTempView("playerStats")

    // 计算每年各项统计数据的平均值和标准差
    val aggStats = spark.sql(
      """
        |SELECT
        |  year,
        |  AVG(`FG%`) AS fgp_avg,
        |  AVG(`FT%`) AS ftp_avg,
        |  AVG(`3P`) AS tp_avg,
        |  AVG(`TRB`) AS trb_avg,
        |  AVG(`AST`) AS ast_avg,
        |  AVG(`STL`) AS stl_avg,
        |  AVG(`BLK`) AS blk_avg,
        |  AVG(`TOV`) AS tov_avg,
        |  AVG(`PTS`) AS pts_avg,
        |  STDDEV(`3P`) AS tp_stddev,
        |  STDDEV(`TRB`) AS trb_stddev,
        |  STDDEV(`AST`) AS ast_stddev,
        |  STDDEV(`STL`) AS stl_stddev,
        |  STDDEV(`BLK`) AS blk_stddev,
        |  STDDEV(`TOV`) AS tov_stddev,
        |  STDDEV(`PTS`) AS pts_stddev
        |FROM playerStats
        |GROUP BY year
      """.stripMargin)

    aggStats.createOrReplaceTempView("aggStats")

    // 计算 Z 分数
    val zScores = spark.sql(
      """
        |SELECT
        |  p.*,
        |  (p.`FG%` - a.fgp_avg) / a.fgp_avg AS zFG,
        |  (p.`FT%` - a.ftp_avg) / a.ftp_avg AS zFT,
        |  (p.`3P` - a.tp_avg) / a.tp_stddev AS z3P,
        |  (p.`TRB` - a.trb_avg) / a.trb_stddev AS zTRB,
        |  (p.`AST` - a.ast_avg) / a.ast_stddev AS zAST,
        |  (p.`STL` - a.stl_avg) / a.stl_stddev AS zSTL,
        |  (p.`BLK` - a.blk_avg) / a.blk_stddev AS zBLK,
        |  (p.`TOV` - a.tov_avg) / a.tov_stddev * (-1) AS zTOV,
        |  (p.`PTS` - a.pts_avg) / a.pts_stddev AS zPTS
        |FROM playerStats p
        |JOIN aggStats a ON p.year = a.year
      """.stripMargin)

    zScores.createOrReplaceTempView("zScores")

    // 计算归一化 Z 分数
    val normZS = spark.sql(
      """
        |SELECT
        |  p.*,
        |  (p.zFG - MIN(p.zFG) OVER (PARTITION BY p.year)) / (MAX(p.zFG) OVER (PARTITION BY p.year) - MIN(p.zFG) OVER (PARTITION BY p.year)) AS nFG,
        |  (p.zFT - MIN(p.zFT) OVER (PARTITION BY p.year)) / (MAX(p.zFT) OVER (PARTITION BY p.year) - MIN(p.zFT) OVER (PARTITION BY p.year)) AS nFT,
        |  (p.z3P - MIN(p.z3P) OVER (PARTITION BY p.year)) / (MAX(p.z3P) OVER (PARTITION BY p.year) - MIN(p.z3P) OVER (PARTITION BY p.year)) AS n3P,
        |  (p.zTRB - MIN(p.zTRB) OVER (PARTITION BY p.year)) / (MAX(p.zTRB) OVER (PARTITION BY p.year) - MIN(p.zTRB) OVER (PARTITION BY p.year)) AS nTRB,
        |  (p.zAST - MIN(p.zAST) OVER (PARTITION BY p.year)) / (MAX(p.zAST) OVER (PARTITION BY p.year) - MIN(p.zAST) OVER (PARTITION BY p.year)) AS nAST,
        |  (p.zSTL - MIN(p.zSTL) OVER (PARTITION BY p.year)) / (MAX(p.zSTL) OVER (PARTITION BY p.year) - MIN(p.zSTL) OVER (PARTITION BY p.year)) AS nSTL,
        |  (p.zBLK - MIN(p.zBLK) OVER (PARTITION BY p.year)) / (MAX(p.zBLK) OVER (PARTITION BY p.year) - MIN(p.zBLK) OVER (PARTITION BY p.year)) AS nBLK,
        |  (p.zTOV - MIN(p.zTOV) OVER (PARTITION BY p.year)) / (MAX(p.zTOV) OVER (PARTITION BY p.year) - MIN(p.zTOV) OVER (PARTITION BY p.year)) AS nTOV,
        |  (p.zPTS - MIN(p.zPTS) OVER (PARTITION BY p.year)) / (MAX(p.zPTS) OVER (PARTITION BY p.year) - MIN(p.zPTS) OVER (PARTITION BY p.year)) AS nPTS
        |FROM zScores p
      """.stripMargin)

    normZS.createOrReplaceTempView("normZS")

    // 计算总 Z 分数和归一化 Z 分数
    val finalScores = spark.sql(
      """
        |SELECT
        |  *,
        |  (zFG + zFT + z3P + zTRB + zAST + zSTL + zBLK + zTOV + zPTS) AS zTOT,
        |  (nFG + nFT + n3P + nTRB + nAST + nSTL + nBLK + nTOV + nPTS) AS nTOT
        |FROM normZS
      """.stripMargin)

    finalScores.createOrReplaceTempView("finalScores")

    // 计算每个球员的最小年龄，用于计算经验值
    val playerStatsZ = spark.sql(
      """
        |SELECT
        |  (p.Age - t.min_age) AS experience,
        |  p.*
        |FROM finalScores p
        |JOIN (SELECT Player, MIN(Age) AS min_age FROM finalScores GROUP BY Player) t
        |ON p.Player = t.Player
      """.stripMargin)

    // 保存结果
    val outputPath = "C:\\Users\\ctg80\\Desktop\\NBA1\\output5\\playerStatsZ"
    playerStatsZ.write.option("header", true).mode(SaveMode.Overwrite).csv(outputPath)

    println("Zscore Calculated")

    spark.stop()
  }
}
